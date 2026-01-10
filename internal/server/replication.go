package server

import (
	"crypto/rand"
	"encoding/hex"
	"goredis/internal/persistant"
	"goredis/pkg/connection"
	"sync"
	"time"
)

const DefaultBacklogSize = 1 << 20 // 1MB，和 Redis 一致

type SlaveInfo struct {
	conn      connection.Connection
	ackOffset int64
	lastAck   time.Time
}

type Replication struct {
	slaves  map[connection.Connection]*SlaveInfo
	backlog *persistant.ReplBacklog
	mu      sync.Mutex
}

func NewReplication() *Replication {
	return &Replication{
		slaves: make(map[connection.Connection]*SlaveInfo),
	}
}

func (r *Replication) InitBacklog(startOffset int64) {
	r.backlog = persistant.NewReplBacklog(DefaultBacklogSize, startOffset)
}

func (r *Replication) AddSlave(conn connection.Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.slaves[conn] = &SlaveInfo{
		conn:      conn,
		ackOffset: 0,
		lastAck:   time.Now(),
	}
}

func (r *Replication) RemoveSlave(c connection.Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slaves, c)
}

func GenReplID() string {
	buf := make([]byte, 20) // 20 bytes = 40 hex chars
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
