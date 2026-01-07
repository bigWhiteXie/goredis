package server

import (
	"goredis/internal/database"
	"goredis/internal/resp"
	"goredis/pkg/parser"
	"net"
)

type Config struct {
	Addr   string
	AOFDir string
	DBNum  int
}

type Server struct {
	cfg Config
	dbs []*database.DB
}

func NewServer(cfg Config) (*Server, error) {
	dbs := make([]*database.DB, cfg.DBNum)
	for i := 0; i < cfg.DBNum; i++ {
		db := database.MakeDB(i, cfg.AOFDir)
		dbs[i] = db
	}

	return &Server{
		cfg: cfg,
		dbs: dbs,
	}, nil
}

func (s *Server) ListenAndServe() error {
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
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(raw net.Conn) {
	client := resp.NewTCPConnection(raw)
	defer client.Close()

	parser := parser.NewParser(raw)

	for {
		payload, err := parser.Parse()
		if err != nil {
			return
		}

		cmdLine := payload.([][]byte)
		db := s.dbs[client.GetDBIndex()]

		reply := db.Exec(client, cmdLine)

		_, err = client.Write(reply.ToBytes())
		if err != nil {
			return
		}
	}
}
