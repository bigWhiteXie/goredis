package server

import (
	"fmt"
	"goredis/internal/common"
	"goredis/pkg/connection"
	"log"
	"strconv"
	"strings"
	"time"
)

// 处理slave的连接
func (s *Server) handlePSync(conn connection.Connection, cmdLine [][]byte) {
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
		s.repl.AddSlave(conn)
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

	s.repl.AddSlave(conn)
	s.aofHandler.AddSlave(conn)
}

func isPSync(cmdLine [][]byte) bool {
	if len(cmdLine) == 0 {
		return false
	}

	return strings.EqualFold(string(cmdLine[0]), "psync")
}

func isReplConf(cmdLine [][]byte) bool {
	return len(cmdLine) >= 1 &&
		strings.EqualFold(string(cmdLine[0]), "replconf")
}

func (s *Server) handleReplConf(
	conn connection.Connection,
	cmdLine [][]byte,
) {
	if len(cmdLine) < 3 {
		return
	}

	sub := strings.ToUpper(string(cmdLine[1]))

	switch sub {
	case "ACK":
		offset, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
		if err != nil {
			return
		}
		s.repl.HandleAck(conn, offset)
	}
}

func (r *Replication) HandleAck(
	conn connection.Connection,
	offset int64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	slave, ok := r.slaves[conn]
	if !ok {
		return
	}

	slave.ackOffset = offset
	slave.lastAck = time.Now()
}
