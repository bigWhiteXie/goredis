package resp

import (
	"errors"
	"net"
	"sync"
)

type TCPConnection struct {
	conn    net.Conn
	dbIndex int

	mu     sync.Mutex
	closed bool
}

func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:    conn,
		dbIndex: 0, // Redis 默认 DB 0
	}
}

func (c *TCPConnection) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, errors.New("connection closed")
	}
	return c.conn.Write(b)
}

func (c *TCPConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

func (c *TCPConnection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *TCPConnection) GetDBIndex() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dbIndex
}

func (c *TCPConnection) SelectDB(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dbIndex = index
}

func (c *TCPConnection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
