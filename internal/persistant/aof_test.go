package persistant

import (
	"bytes"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type MockDB struct {
	data  map[string]*types.DataEntity
	ttl   map[string]time.Time
	index int
	mu    sync.RWMutex
}

func NewMockDB(index int) *MockDB {
	return &MockDB{
		data:  make(map[string]*types.DataEntity),
		ttl:   make(map[string]time.Time),
		index: index,
	}
}

func (m *MockDB) GetEntity(key string) (*types.DataEntity, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.data[key]
	return e, ok
}

func (m *MockDB) PutEntity(key string, entity *types.DataEntity) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = entity
	return 1
}

func (m *MockDB) GetDBIndex() int { return m.index }

func (m *MockDB) ForEach(fn func(string, types.RedisData)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		fn(k, v.Data.(types.RedisData))
	}
}

func (m *MockDB) Remove(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.ttl, key)
	return true
}

func (m *MockDB) SetExpire(key string, expireTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttl[key] = expireTime
}

func (m *MockDB) GetExpireTime(key string) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.ttl[key]; ok {
		return t, true
	}
	return time.Time{}, false
}

// 其他方法留空
func (m *MockDB) Exec(c connection.Connection, cmdLine [][]byte) resp.Reply { panic("not implemented") }
func (m *MockDB) IsExpired(k string) bool                                   { return false }
func (m *MockDB) StartExpireTask()                                          {}
func (m *MockDB) DeleteTTL(k string)                                        {}

func TestAOFHandler(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "db0.aof")

	t.Run("NewAOFHandler creates file", func(t *testing.T) {
		aof, err := NewAOFHandler(tempDir, 0)
		if err != nil {
			t.Fatalf("NewAOFHandler failed: %v", err)
		}
		defer aof.file.Close()

		if _, err := os.Stat(aofPath); err != nil {
			t.Errorf("AOF file not created")
		}
	})

	t.Run("AddAOF and Load", func(t *testing.T) {
		aof, err := NewAOFHandler(tempDir, 1)
		if err != nil {
			t.Fatalf("NewAOFHandler failed: %v", err)
		}
		defer aof.file.Close()

		// Add commands
		cmd1 := [][]byte{[]byte("set"), []byte("k1"), []byte("v1")}
		cmd2 := [][]byte{[]byte("hset"), []byte("h1"), []byte("f1"), []byte("v1")}
		aof.AddAOF(cmd1)
		aof.AddAOF(cmd2)

		// Wait for handle to process
		time.Sleep(100 * time.Millisecond)
		aof.flush()

		// Load and verify
		var loaded [][]byte
		err = aof.Load(func(cmd types.CmdLine) {
			loaded = append(loaded, cmd...)
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loaded) != 7 { // set k1 v1 hset h1 f1 v1 → 3+4=7
			t.Errorf("unexpected loaded commands: %v", loaded)
		}
	})

	t.Run("Rewrite", func(t *testing.T) {
		aof, err := NewAOFHandler(tempDir, 2)
		if err != nil {
			t.Fatalf("NewAOFHandler failed: %v", err)
		}
		defer aof.file.Close()

		// Add some commands
		aof.AddAOF([][]byte{[]byte("set"), []byte("k1"), []byte("v1")})
		aof.AddAOF([][]byte{[]byte("del"), []byte("k1")})
		time.Sleep(100 * time.Millisecond)
		aof.flush()

		// Create mock DB with final state
		db := NewMockDB(2)
		db.PutEntity("k2", &types.DataEntity{Data: &MockString{"final"}})

		// Rewrite
		err = aof.Rewrite(db)
		if err != nil {
			t.Fatalf("Rewrite failed: %v", err)
		}

		// Verify new AOF only contains final state
		content, err := os.ReadFile(aof.path)
		if err != nil {
			t.Fatalf("Read AOF failed: %v", err)
		}

		// Should contain "set k2 final"
		if !bytes.Contains(content, []byte("k2")) || !bytes.Contains(content, []byte("final")) {
			t.Errorf("Rewrite did not write final state: %s", content)
		}
		// Should NOT contain "k1" (deleted)
		if bytes.Contains(content, []byte("k1")) {
			t.Errorf("Rewrite should not contain deleted key: %s", content)
		}
	})

	t.Run("HasData", func(t *testing.T) {
		aof, err := NewAOFHandler(tempDir, 3)
		if err != nil {
			t.Fatalf("NewAOFHandler failed: %v", err)
		}
		defer aof.file.Close()

		if aof.HasData() {
			t.Error("new AOF should be empty")
		}

		aof.AddAOF([][]byte{[]byte("set"), []byte("a"), []byte("b")})
		time.Sleep(100 * time.Millisecond)
		aof.flush()

		if !aof.HasData() {
			t.Error("AOF should have data after write")
		}
	})

	t.Run("LogSize", func(t *testing.T) {
		aof, err := NewAOFHandler(tempDir, 4)
		if err != nil {
			t.Fatalf("NewAOFHandler failed: %v", err)
		}
		defer aof.file.Close()

		size, _ := aof.LogSize()
		if size != 0 {
			t.Errorf("initial size should be 0, got %d", size)
		}

		aof.AddAOF([][]byte{[]byte("set"), []byte("x"), []byte("y")})
		time.Sleep(100 * time.Millisecond)
		aof.flush()

		size, _ = aof.LogSize()
		if size == 0 {
			t.Error("size should be >0 after write")
		}
	})
}

// MockString for RedisData
type MockString struct {
	val string
}

func (m *MockString) ToWriteCmdLine(key string) [][]byte {
	return [][]byte{[]byte("set"), []byte(key), []byte(m.val)}
}

func (m *MockString) Clone() interface{} {
	return &MockString{val: m.val}
}
