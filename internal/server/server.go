package server

import (
	"fmt"
	"goredis/internal/common"
	"goredis/internal/database"
	"goredis/internal/persistant"
	"goredis/pkg/connection"
	"goredis/pkg/parser"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// 当作为从节点运行时的上下文
type SlaveState struct {
	masterAddr   string
	masterReplID string
	offset       int64

	conn net.Conn
}

type Config struct {
	Addr       string
	AOFDir     string
	DBNum      int    // 当前只支持使用0号数据库
	MasterAddr string // 非空表示 slave
}

type Server struct {
	cfg  Config
	repl *Replication
	db   *database.DB

	repliID    string
	aofHandler *persistant.AOFHandler

	slave *SlaveState
}

func NewServer(cfg Config) (*Server, error) {
	aofHandler, err := persistant.NewAOFHandler(cfg.AOFDir, 0)
	if err != nil {
		panic(err)
	}
	// redis的主从架构是多层的，每个节点都可能是主节点，因此都需要构造Replication
	repl := NewReplication()
	repl.InitBacklog(aofHandler.CurrentOffset())

	aofHandler.SetBacklog(repl.backlog)
	s := &Server{
		cfg:        cfg,
		db:         database.MakeDB(0, aofHandler),
		repl:       NewReplication(),
		repliID:    GenReplID(),
		aofHandler: aofHandler,
	}

	// 启动时都执行全量加载
	if cfg.MasterAddr != "" {
		log.Printf("[slave] master has been set %s", cfg.MasterAddr)
		s.slave = &SlaveState{
			masterAddr:   cfg.MasterAddr,
			masterReplID: "?",
			offset:       0,
		}
	}

	return s, nil
}

func (s *Server) ListenAndServe() error {
	if s.slave != nil {
		go s.startReplicationAsSlave()
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		log.Printf("[server] accept connect success")
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(raw net.Conn) {
	client := connection.NewTCPConnection(raw)
	defer func() {
		// 断开时清理 slave
		if client.IsSlave() {
			s.repl.RemoveSlave(client)
		}
		client.Close()
	}()
	parser := parser.NewParser(raw)

	for {
		payload, err := parser.Parse()
		if err != nil {
			return
		}

		cmdLine, ok := common.ToCmdLine(payload)
		if !ok {
			log.Printf("[server] invalid payload type: %T", payload)
			return
		}
		common.LogBytesArr("server", cmdLine)
		// 处理slave连接的数据同步问题
		if isPSync(cmdLine) {
			s.handlePSync(client, cmdLine)
			continue
		}

		reply := s.db.Exec(client, cmdLine)

		_, err = client.Write(reply.ToBytes())
		if err != nil {
			return
		}
	}
}

// 处理slave的连接
func (s *Server) handlePSync(conn *connection.TCPConnection, cmdLine [][]byte) {
	var slaveReplID string
	var slaveOffset int64

	if len(cmdLine) >= 3 {
		slaveReplID = string(cmdLine[1])
		slaveOffset, _ = strconv.ParseInt(string(cmdLine[2]), 10, 64)
	}

	common.LogBytesArr("recived slave", cmdLine)
	// 尝试 partial resync
	if slaveReplID == s.repliID &&
		s.repl.backlog != nil &&
		s.repl.backlog.CanServe(slaveOffset) {

		conn.Write([]byte("+CONTINUE\r\n"))
		data := s.repl.backlog.ReadFrom(slaveOffset)
		conn.Write(data)

		s.aofHandler.AddSlave(conn)
		return
	}

	// FULLRESYNC
	data, startOffset, err := s.aofHandler.ReadAll()
	if err != nil {
		log.Printf("[psync] fail to exec full resync:%s", err)
		return
	}
	conn.Write([]byte(fmt.Sprintf(
		"+FULLRESYNC %s %d\r\n", s.repliID, startOffset,
	)))
	conn.Write(data)

	s.aofHandler.AddSlave(conn)
}

// 作为从节点开始向主节点发起注册，并循环处理主节点的写命令
func (s *Server) startReplicationAsSlave() {
	for {
		conn, err := net.Dial("tcp", s.slave.masterAddr)
		if err != nil {
			log.Printf("connect master failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		s.slave.conn = conn

		// 发送 PSYNC
		cmd := fmt.Sprintf(
			"*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n",
			len(s.slave.masterReplID), s.slave.masterReplID,
			len(strconv.FormatInt(s.slave.offset, 10)),
			s.slave.offset,
		)
		conn.Write([]byte(cmd))

		s.handleMasterStream(conn)

		// 这里说明连接中断
		time.Sleep(2 * time.Second)
	}
}

// 阻塞方法：在conn中断前会一直循环处理主节点的命令
func (s *Server) handleMasterStream(conn net.Conn) {
	parser := parser.NewParser(conn)

	// 读取 FULLRESYNC / CONTINUE
	payload, _ := parser.Parse()
	log.Printf("[slave] first master cmd:%v", payload)
	cmdLine, ok := common.ToCmdLine(payload)
	if !ok {
		log.Print("[slave] fail to handle payload, exit loop")
		return
	}
	log.Printf("[slave] success handle first master cmd:%s", string(cmdLine[0]))

	if strings.HasPrefix(string(cmdLine[0]), "FULLRESYNC") {
		s.slave.masterReplID = string(cmdLine[1])
		s.slave.offset, _ = strconv.ParseInt(string(cmdLine[2]), 10, 64)

		log.Printf("[slave] masterID:%s start offset :%d", s.slave.masterReplID, s.slave.offset)
		// 清空本地状态
		s.db.Clear()
		s.aofHandler.Reset(s.slave.offset)
		s.repl.InitBacklog(s.slave.offset)
		// 从全量复制开始执行并持续监听后续增量写命令
		s.replayFromMaster(parser, conn)

	} else if strings.HasPrefix(string(cmdLine[0]), "CONTINUE") {
		// 直接进入增量 replay
		s.replLoop(parser, conn)
	}
}

func (s *Server) replayFromMaster(parser *parser.Parser, conn net.Conn) {
	tcpConn := connection.NewTCPConnection(conn)

	for {
		payload, err := parser.Parse()
		if err != nil {
			// master 关闭 or 网络异常
			log.Printf("[slave] replay from master stopped: %v", err)
			return
		}

		cmdLine, ok := common.ToCmdLine(payload)
		if !ok {
			log.Printf("[slave] invalid payload during replay")
			continue
		}

		common.LogBytesArr("[slave replay]", cmdLine)

		// 1. 执行到本地 DB
		reply := s.db.Exec(tcpConn, cmdLine)
		delta := int64(len(reply.ToBytes()))
		s.slave.offset += delta
	}
}

func (s *Server) replLoop(parser *parser.Parser, conn net.Conn) {
	for {
		payload, err := parser.Parse()
		if err != nil {
			return
		}

		cmdLine, ok := common.ToCmdLine(payload)
		if !ok {
			continue
		}
		common.LogBytesArr("slave", cmdLine)
		// 直接执行，不返回结果
		reply := s.db.Exec(connection.NewTCPConnection(conn), cmdLine)
		delta := int64(len(reply.ToBytes()))
		s.slave.offset += delta
	}
}

func isPSync(cmdLine [][]byte) bool {
	if len(cmdLine) == 0 {
		return false
	}

	return strings.EqualFold(string(cmdLine[0]), "psync")
}
