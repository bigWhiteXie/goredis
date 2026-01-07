package persistant

import (
	"bufio"
	"fmt"
	"goredis/internal/resp"
	"goredis/internal/types"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	batchSize = 1024
)

type AOFHandler struct {
	file   *os.File
	writer *bufio.Writer
	ch     chan types.CmdLine

	mu          sync.Mutex
	bufferCount int
}

func NewAOFHandler(dir string, dbIndex int) (*AOFHandler, error) {
	path := filepath.Join(dir, fmt.Sprintf("db%d.aof", dbIndex))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	h := &AOFHandler{
		file:   file,
		writer: bufio.NewWriter(file),
		ch:     make(chan types.CmdLine, 4096),
	}

	go h.handle()
	return h, nil
}

func (aof *AOFHandler) AddAOF(cmd types.CmdLine) {
	select {
	case aof.ch <- cmd:
	default:
		go func() {
			aof.ch <- cmd
		}()
	}
}

func (aof *AOFHandler) handle() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case cmd := <-aof.ch:
			aof.writeCmd(cmd)
			aof.bufferCount++

			if aof.bufferCount >= batchSize {
				aof.flush()
			}

		case <-ticker.C:
			if aof.bufferCount > 0 {
				aof.flush()
			}
		}
	}
}

func (aof *AOFHandler) writeCmd(cmd types.CmdLine) {
	reply := resp.MakeMultiBulkReply(cmd)
	_, _ = aof.writer.Write(reply.ToBytes())
}

func (h *AOFHandler) flush() {
	if err := h.writer.Flush(); err != nil {
		log.Printf("aof flush failed: %v", err)
	}
	if err := h.file.Sync(); err != nil {
		log.Printf("aof fsync failed: %v", err)
	}
	h.bufferCount = 0
}
