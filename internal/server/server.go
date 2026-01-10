package server

import (
	"goredis/internal/common"
	"goredis/internal/database"
	"goredis/internal/persistant"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
	"goredis/pkg/parser"
	"log"
	"net"
)

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
		if isSlaveCmd := s.handleSlaveCmd(client, cmdLine); isSlaveCmd {
			common.LogBytesArr("server", cmdLine)
			continue
		}
		if cmd := types.CmdLine(cmdLine); cmd.IsWrite() && s.slave != nil {
			log.Println("[slave] can't exec write cmd")
			errReply := resp.MakeErrReply("slave can't execute write cmd")
			client.Write(errReply.ToBytes())
			continue
		}

		reply := s.db.Exec(client, cmdLine)
		_, err = client.Write(reply.ToBytes())
		if err != nil {
			return
		}
	}
}

func (s *Server) handleSlaveCmd(client connection.Connection, cmdLine [][]byte) bool {
	// 处理slave连接的数据同步问题
	if isPSync(cmdLine) {
		s.handlePSync(client, cmdLine)
		return true
	}
	if isReplConf(cmdLine) {
		s.handleReplConf(client, cmdLine)
		return true
	}

	return false
}
