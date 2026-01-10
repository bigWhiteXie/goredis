package persistant

import (
	"bufio"
	"fmt"
	"goredis/internal/common"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
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
	path   string
	file   *os.File      // aof文件
	writer *bufio.Writer // 缓冲区
	ch     chan types.CmdLine

	mu          sync.Mutex
	bufferCount int
	state       int32
	rewriteBuf  []types.CmdLine // 存放rewrite期间的新命令

	// 主从集群相关字段
	offset   int64 // 当前实例写入aof文件的偏移量
	slavesMu sync.Mutex
	slaves   map[connection.Connection]struct{}
	backlog  *ReplBacklog
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
	h.slaves = make(map[connection.Connection]struct{})
	h.offset, _ = h.LogSize()

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

func (aof *AOFHandler) SetBacklog(backlog *ReplBacklog) {
	aof.backlog = backlog
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

func (aof *AOFHandler) CurrentOffset() int64 {
	return atomic.LoadInt64(&aof.offset)
}

func (aof *AOFHandler) ReadAll() ([]byte, int64, error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	bytes, err := os.ReadFile(aof.path)
	if err != nil {
		return nil, 0, err
	}

	return bytes, aof.backlog.end - int64(len(bytes)), nil
}

func (aof *AOFHandler) AddSlave(w connection.Connection) {
	aof.slavesMu.Lock()
	defer aof.slavesMu.Unlock()
	aof.slaves[w] = struct{}{}
}

func (aof *AOFHandler) RemoveSlave(w connection.Connection) {
	aof.slavesMu.Lock()
	defer aof.slavesMu.Unlock()
	delete(aof.slaves, w)
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
	b := reply.ToBytes()

	// 原子更新aof的offset和backlog的offset
	aof.mu.Lock()
	n, _ := aof.writer.Write(b)
	aof.offset += int64(n)
	if aof.backlog != nil {
		aof.backlog.Append(b)
	}
	aof.mu.Unlock()

	// 向从节点广播命令
	aof.slavesMu.Lock()
	for s := range aof.slaves {
		if _, err := s.Write(b); err != nil {
			log.Printf("[aof replication] write cmd failed: %s", err)
			delete(aof.slaves, s)
		}
	}
	aof.slavesMu.Unlock()
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

// Reset 清空 AOF 文件并重置内部状态
func (aof *AOFHandler) Reset(offset int64) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// 1. 先把缓冲区刷掉，避免旧数据混进去
	if err := aof.writer.Flush(); err != nil {
		return fmt.Errorf("flush writer failed: %w", err)
	}

	// 2. 截断文件（如果不存在则创建）
	if err := aof.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate aof failed: %w", err)
	}

	// 3. 文件指针回到头部
	if _, err := aof.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek aof failed: %w", err)
	}

	// 4. 强制落盘
	if err := aof.file.Sync(); err != nil {
		return fmt.Errorf("fsync aof failed: %w", err)
	}

	// 5. 重置内部状态
	aof.bufferCount = 0
	aof.offset = offset
	aof.rewriteBuf = make([]types.CmdLine, 0)
	atomic.StoreInt64(&aof.offset, 0)

	return nil
}
