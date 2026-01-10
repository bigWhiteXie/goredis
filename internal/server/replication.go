package server

import (
	"crypto/rand"
	"encoding/hex"
	"goredis/internal/persistant"
	"goredis/pkg/connection"
	"sync"
)

const DefaultBacklogSize = 1 << 20 // 1MB，和 Redis 一致

type Replication struct {
	mu      sync.Mutex
	slaves  map[*connection.TCPConnection]struct{}
	backlog *persistant.ReplBacklog
}

func NewReplication() *Replication {
	return &Replication{
		slaves: make(map[*connection.TCPConnection]struct{}),
	}
}

func (r *Replication) InitBacklog(startOffset int64) {
	r.backlog = persistant.NewReplBacklog(DefaultBacklogSize, startOffset)
}

func (r *Replication) AddSlave(c *connection.TCPConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c.SetSlave()
	r.slaves[c] = struct{}{}
}

func (r *Replication) RemoveSlave(c *connection.TCPConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slaves, c)
}

func GenReplID() string {
	buf := make([]byte, 20) // 20 bytes = 40 hex chars
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
