// db_test.go
package database

import (
	"fmt"
	"goredis/internal/command"
	"goredis/internal/data"
	"goredis/internal/persistant"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
	"sync"
	"testing"
	"time"
)

// MockAOFHandler 实现内存版 AOF
type MockAOFHandler struct {
	mu      sync.Mutex
	log     []types.CmdLine
	hasData bool
}

func NewMockAOFHandler() *MockAOFHandler {
	return &MockAOFHandler{}
}

func (m *MockAOFHandler) AddAOF(cmd types.CmdLine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, cmd)
	m.hasData = true
}

func (m *MockAOFHandler) HasData() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasData
}

func (m *MockAOFHandler) Load(replay func(cmd types.CmdLine)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cmd := range m.log {
		replay(cmd)
	}
	return nil
}

func (m *MockAOFHandler) Rewrite(db types.Database) error      { return nil }
func (m *MockAOFHandler) LogSize() (int64, error)              { return 0, nil }
func (m *MockAOFHandler) SetBacklog(b *persistant.ReplBacklog) {}
func (m *MockAOFHandler) CurrentOffset() int64                 { return 0 }
func (m *MockAOFHandler) ReadAll() ([]byte, int64, error)      { return nil, 0, nil }
func (m *MockAOFHandler) AddSlave(w connection.Connection)     {}
func (m *MockAOFHandler) RemoveSlave(w connection.Connection)  {}
func (m *MockAOFHandler) SetState(state int32)                 {}
func (m *MockAOFHandler) Reset(offset int64) error             { return nil }

func initTest() {
	// 注册测试命令
	command.RegisterCommand(&command.Command{
		Name: "set",
		Executor: func(db types.Database, args [][]byte) resp.Reply {
			if len(args) != 2 {
				return resp.MakeErrReply("ERR wrong number of arguments")
			}
			key := string(args[0])
			value := args[1]

			// 简单字符串存储
			db.PutEntity(key, &types.DataEntity{
				Data: &MockString{value: string(value)},
			})
			return resp.MakeOkReply()
		},
		Arity: 3, // SET key value
	})

	command.RegisterCommand(&command.Command{
		Name: "get",
		Executor: func(db types.Database, args [][]byte) resp.Reply {
			if len(args) != 1 {
				return resp.MakeErrReply("ERR wrong number of arguments")
			}
			key := string(args[0])
			entity, ok := db.GetEntity(key)
			if !ok {
				return resp.MakeNullBulkReply()
			}
			str := entity.Data.(*MockString)
			return resp.MakeBulkReply([]byte(str.value))
		},
		Arity: 2,
	})
}

type MockString struct {
	value string
}

func (m *MockString) ToWriteCmdLine(key string) [][]byte {
	return [][]byte{[]byte("set"), []byte(key), []byte(m.value)}
}

func (m *MockString) Clone() interface{} {
	return &MockString{value: m.value}
}

func TestDB(t *testing.T) {
	aof := NewMockAOFHandler()
	db := MakeDB(0, aof)

	t.Run("PutEntity and GetEntity", func(t *testing.T) {
		entity := &types.DataEntity{Data: &MockString{"test"}}
		db.PutEntity("key1", entity)

		got, ok := db.GetEntity("key1")
		if !ok || got.Data.(*MockString).value != "test" {
			t.Error("GetEntity failed")
		}
	})

	t.Run("Remove", func(t *testing.T) {
		db.PutEntity("key2", &types.DataEntity{Data: &MockString{"remove"}})
		if !db.Remove("key2") {
			t.Error("Remove should return true")
		}
		if _, ok := db.GetEntity("key2"); ok {
			t.Error("key2 should be deleted")
		}
	})

	t.Run("TTL and IsExpired", func(t *testing.T) {
		db.PutEntity("ttlkey", &types.DataEntity{Data: &MockString{"ttl"}})
		expireTime := time.Now().Add(100 * time.Millisecond)
		db.SetExpire("ttlkey", expireTime)
		time.Sleep(100 * time.Millisecond)
		if !db.IsExpired("ttlkey") {
			t.Error("should not be expired yet")
		}

		time.Sleep(150 * time.Millisecond)
		if !db.IsExpired("ttlkey") {
			t.Error("should be expired now")
		}

		// GetEntity should auto-delete expired key
		if _, ok := db.GetEntity("ttlkey"); ok {
			t.Error("expired key should be auto-deleted")
		}
	})

	t.Run("Exec command", func(t *testing.T) {
		// Use real connection (not AOFConnection)
		conn := &MockConnection{}

		// SET key value
		reply := db.Exec(conn, [][]byte{[]byte("set"), []byte("cmdkey"), []byte("cmdval")})
		if !isOKReply(reply) {
			t.Errorf("SET failed: %s", getErrorString(reply))
		}

		// GET key
		reply = db.Exec(conn, [][]byte{[]byte("get"), []byte("cmdkey")})
		if string(getBulkValue(reply)) != "cmdval" {
			t.Errorf("GET returned wrong value: %q", getBulkValue(reply))
		}

		// Verify AOF logged
		if len(aof.log) != 2 {
			t.Errorf("expected 2 AOF entries, got %d", len(aof.log))
		}
	})

	t.Run("Exec with AOFConnection skips AOF", func(t *testing.T) {
		aof.log = nil // reset
		conn := connection.NewAOFConnection(0)

		db.Exec(conn, [][]byte{[]byte("set"), []byte("aofkey"), []byte("aofval")})

		if len(aof.log) != 0 {
			t.Error("AOFConnection should not log to AOF")
		}
	})

	t.Run("Clone", func(t *testing.T) {
		db.PutEntity("clonekey", &types.DataEntity{Data: data.NewStringFromBytes([]byte("hello"))})
		expire := time.Now().Add(time.Hour)
		db.SetExpire("clonekey", expire)

		clone := db.Clone()

		// Verify data
		entity, ok := clone.GetEntity("clonekey")
		if !ok || string(entity.Data.(*data.SimpleString).Get()) != "hello" {
			t.Error("clone data mismatch")
		}

		// Verify TTL
		if expireTime, ok := clone.GetExpireTime("clonekey"); !ok || !expireTime.Equal(expire) {
			t.Error("clone TTL mismatch")
		}

		// Modify original should not affect clone
		db.Remove("clonekey")
		if _, ok := clone.GetEntity("clonekey"); !ok {
			t.Error("clone should be independent")
		}
	})

	t.Run("activeExpire", func(t *testing.T) {
		// Add multiple keys with different TTLs
		for i := 0; i < 30; i++ { // > sampleSize(20)
			key := fmt.Sprintf("exp%d", i)
			db.PutEntity(key, &types.DataEntity{Data: &MockString{key}})
			if i%2 == 0 {
				// Even keys expire soon
				db.SetExpire(key, time.Now().Add(10*time.Millisecond))
			} else {
				// Odd keys expire later
				db.SetExpire(key, time.Now().Add(time.Hour))
			}
		}

		time.Sleep(50 * time.Millisecond)
		db.activeExpire() // manually trigger

		// Check some expired keys are deleted
		for i := 0; i < 10; i += 2 {
			key := fmt.Sprintf("exp%d", i)
			if _, ok := db.GetEntity(key); ok {
				t.Errorf("key %s should be expired", key)
			}
		}
	})
}

// Helper functions (adapted for testing)
func isOKReply(reply resp.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

func getBulkValue(reply resp.Reply) []byte {
	bultReply, _ := reply.(*resp.BulkReply)

	return bultReply.Arg
}

func getErrorString(reply resp.Reply) string {
	bytes := reply.ToBytes()
	if len(bytes) > 0 && bytes[0] == '-' {
		return string(bytes[1 : len(bytes)-2])
	}
	return ""
}

// MockConnection for testing
type MockConnection struct{}

func (m *MockConnection) Write([]byte) (int, error) { return 0, nil }
func (m *MockConnection) Close() error              { return nil }
func (m *MockConnection) GetDBIndex() int           { return 0 }
func (m *MockConnection) IsClosed() bool            { return false }
func (m *MockConnection) IsSlave() bool             { return false }
func (m *MockConnection) RemoteAddr() string        { return "mock" }
func (m *MockConnection) SelectDB(index int)        {}

func (m *MockConnection) SetSlave() {}
