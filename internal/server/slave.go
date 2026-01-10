package server

import (
	"errors"
	"fmt"
	"goredis/internal/common"
	"goredis/pkg/connection"
	"goredis/pkg/parser"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type SlaveState struct {
	masterAddr   string
	masterReplID string
	offset       int64

	conn   net.Conn
	closed bool
}

func (state *SlaveState) SetOffset(offset int64) {
	atomic.StoreInt64(&state.offset, offset)
}

func (state *SlaveState) GetOffset() int64 {
	return atomic.LoadInt64(&state.offset)
}

func (s *Server) startReplicationAsSlave() {
	for {
		if err := s.slaveOnce(); err != nil {
			log.Printf("[slave] replication error: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func (s *Server) slaveOnce() error {
	conn, err := net.Dial("tcp", s.slave.masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	s.slave.conn = conn

	parser := parser.NewParser(conn)

	// 1. PSYNC
	if _, err := s.sendPSync(conn); err != nil {
		return err
	}

	// 2. 解析 master 首包
	payload, err := parser.Parse()
	if err != nil {
		log.Printf("[slave] parse payload failed for: %s", err)
		return err
	}
	log.Printf("[slave] first master cmd:%v", payload)
	cmdLine, ok := common.ToCmdLine(payload)
	if !ok {
		log.Print("[slave] fail to handle payload, exit loop")
		return errors.New("[slave] fail to convert payload to cmd")
	}
	log.Printf("[slave] success handle first master cmd:%s", string(cmdLine[0]))

	if strings.HasPrefix(string(cmdLine[0]), "FULLRESYNC") {
		// 从全量复制开始执行并持续监听后续增量写命令
		return s.handleFullResync(cmdLine, parser)

	} else if strings.HasPrefix(string(cmdLine[0]), "CONTINUE") {
		// 直接进入增量 replay
		s.replicationLoop(parser)
	}

	return fmt.Errorf("unexpected master reply")
}

func (s *Server) handleFullResync(
	cmdLine [][]byte,
	parser *parser.Parser,
) error {
	s.slave.masterReplID = string(cmdLine[1])
	s.slave.offset, _ = strconv.ParseInt(string(cmdLine[2]), 10, 64)
	log.Printf("[slave] masterID:%s start offset :%d", s.slave.masterReplID, s.slave.offset)

	// 清空本地状态
	s.db.Clear()
	s.aofHandler.Reset(s.slave.offset)
	s.repl.InitBacklog(s.slave.offset)

	// 进入统一复制流（全量数据 + 增量命令）
	return s.replicationLoop(parser)
}

func (s *Server) replicationLoop(parser *parser.Parser) error {
	tcpConn := connection.NewTCPConnection(s.slave.conn)

	ackTicker := time.NewTicker(3 * time.Second)
	defer ackTicker.Stop()

	for {
		select {
		case <-ackTicker.C:
			s.sendAck()

		default:
			payload, err := parser.Parse()
			if err != nil {
				log.Printf("[slave] read byte from master failed for %s, try to reconnect....", err)
				return err
			}

			cmdLine, ok := common.ToCmdLine(payload)
			if !ok {
				continue
			}
			common.LogBytesArr("slave recived", cmdLine)
			// 执行命令（写 DB + AOF）
			s.db.Exec(tcpConn, cmdLine)

			// 更新offset(使用aof的offset保证数据不丢失)
			s.slave.SetOffset(s.aofHandler.CurrentOffset())
		}
	}
}

func (s *Server) sendPSync(conn net.Conn) (int, error) {
	cmd := fmt.Sprintf(
		"*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n",
		len(s.slave.masterReplID), s.slave.masterReplID,
		len(strconv.FormatInt(s.slave.offset, 10)),
		s.slave.offset,
	)

	return conn.Write([]byte(cmd))
}

func (s *Server) sendAck() {
	if s.slave.conn == nil {
		return
	}

	offsetStr := strconv.FormatInt(s.slave.offset, 10)
	cmd := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n",
		len(offsetStr),
		offsetStr,
	)

	s.slave.conn.Write([]byte(cmd))
}
