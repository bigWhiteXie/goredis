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
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	batchSize = 1024
)

const (
	AOFNormal    = 0
	AOFRewriting = 1
)

type AOFHandler struct {
	file        *os.File      // aof文件
	writer      *bufio.Writer // 缓冲区
	ch          chan types.CmdLine
	mu          sync.Mutex
	path        string
	bufferCount int
	state       int32
	rewriteBuf  []types.CmdLine // 存放rewrite期间的新命令
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

func (aof *AOFHandler) Rewrite(db types.Database) error {
	aof.mu.Lock()
	if aof.state == AOFRewriting {
		aof.mu.Unlock()
		return nil
	}
	aof.state = AOFRewriting
	aof.mu.Unlock()

	tmpPath := fmt.Sprintf("db%d.aof.tmp", db.GetDBIndex())
	tmpFile, _ := os.Create(tmpPath)
	writer := bufio.NewWriter(tmpFile)

	// 写快照
	db.ForEach(func(key string, entity types.RedisData) {
		var ttl float64
		if expiredTime, ok := db.GetExpireTime(key); ok {
			ttl = expiredTime.Sub(time.Now()).Seconds()
			if ttl <= 0 {
				return
			}
		}

		cmd := entity.ToWriteCmdLine(key)
		reply := resp.MakeMultiBulkReply(cmd)
		writer.Write(reply.ToBytes())
		// 该key有过期时间则加上
		if ttl > 0 {
			ttlResp := resp.MakeMultiBulkReply([][]byte{[]byte("expire"), []byte(key), []byte(strconv.FormatFloat(ttl, 'f', -1, 64))})
			writer.Write(ttlResp.ToBytes())
		}
	})

	// 2写 rewrite buffer
	aof.mu.Lock()
	for _, cmd := range aof.rewriteBuf {
		reply := resp.MakeMultiBulkReply(cmd)
		writer.Write(reply.ToBytes())
	}
	aof.rewriteBuf = nil
	aof.mu.Unlock()

	writer.Flush()
	tmpFile.Sync()
	tmpFile.Close()

	// 原子替换
	os.Rename(tmpPath, aof.file.Name())

	aof.mu.Lock()
	aof.state = AOFNormal
	aof.mu.Unlock()

	return nil
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

			if atomic.LoadInt32(&aof.state) == AOFNormal {
				aof.writeCmd(cmd)
			} else {
				// rewrite 期间写入buffer中
				aof.mu.Lock()
				aof.rewriteBuf = append(aof.rewriteBuf, cmd)
				aof.mu.Unlock()
			}
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

func (aof *AOFHandler) SetState(state int32) {
	atomic.StoreInt32(&aof.state, state)
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

func (aof *AOFHandler) LogSize() (int64, error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if aof.file == nil {
		return 0, nil
	}

	info, err := aof.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
