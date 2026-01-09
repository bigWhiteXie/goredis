package persistant

import (
	"bufio"
	"fmt"
	"goredis/internal/common"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/parser"
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
	file        *os.File
	writer      *bufio.Writer
	ch          chan types.CmdLine
	mu          sync.Mutex
	path        string
	bufferCount int
}

func NewAOFHandler(dir string, dbIndex int) (*AOFHandler, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, fmt.Sprintf("db%d.aof", dbIndex))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	h := &AOFHandler{
		file:   file,
		writer: bufio.NewWriter(file),
		ch:     make(chan types.CmdLine, 4096),
		path:   path,
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
			if !cmd.IsWrite() {
				continue
			}
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

func (aof *AOFHandler) HasData() bool {
	info, err := os.Stat(aof.path)
	if err != nil {
		return false
	}
	return info.Size() > 0
}

func (aof *AOFHandler) Load(replay func(cmd types.CmdLine)) error {
	file, err := os.Open(aof.path)
	if err != nil {
		return err
	}
	defer file.Close()

	parser := parser.NewParser(file)

	for {
		payload, err := parser.Parse()
		if err != nil {
			// EOF 是正常结束
			break
		}

		cmdLine, ok := common.ToCmdLine(payload)
		common.LogBytesArr("aof reload", cmdLine)
		if !ok {
			continue
		}

		replay(cmdLine)
	}
	return nil
}
