// helpers_test.go (或直接写在 command_test.go 里)
package command

import (
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
	"strings"
	"sync"
	"testing"
	"time"
)

type MockDB struct {
	data map[string]*types.DataEntity
	ttl  map[string]time.Time
	mu   sync.RWMutex
}

func NewMockDB() *MockDB {
	return &MockDB{
		data: make(map[string]*types.DataEntity),
		ttl:  make(map[string]time.Time),
	}
}

func (m *MockDB) GetEntity(key string) (*types.DataEntity, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查是否过期
	if expireTime, ok := m.ttl[key]; ok && time.Now().After(expireTime) {
		return nil, false
	}

	entity, ok := m.data[key]
	return entity, ok
}

func (m *MockDB) PutEntity(key string, entity *types.DataEntity) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = entity
	return 1
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

func (m *MockDB) IsExpired(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if expireTime, ok := m.ttl[key]; ok {
		return time.Now().After(expireTime)
	}
	return false
}

// 其他接口方法留空或 panic（因为我们不会调用它们）
func (m *MockDB) GetDBIndex() int                                           { return 0 }
func (m *MockDB) ForEach(handler func(string, types.RedisData))             {}
func (m *MockDB) Exec(c connection.Connection, cmdLine [][]byte) resp.Reply { panic("not implemented") }
func (m *MockDB) StartExpireTask()                                          {}
func (m *MockDB) DeleteTTL(key string)                                      {}
func (m *MockDB) GetExpireTime(key string) (time.Time, bool) {
	expire, ok := m.ttl[key]
	return expire, ok
}

// 断言是 IntReply 且值等于 expected
func assertIntReply(t *testing.T, reply resp.Reply, expected int64) {
	t.Helper()
	intReply, ok := reply.(*resp.IntReply)
	if !ok {
		t.Fatalf("expected *resp.IntReply, got %T", reply)
	}
	if intReply.IntVal != expected {
		t.Fatalf("expected int %d, got %d", expected, intReply.IntVal)
	}
}

// 断言是 BulkReply 且内容等于 expected（nil 表示 Null）
func assertBulkReply(t *testing.T, reply resp.Reply, expected []byte) {
	t.Helper()
	bulk, ok := reply.(*resp.BulkReply)
	if !ok {
		t.Fatalf("expected *resp.BulkReply, got %T", reply)
	}
	if expected == nil {
		if bulk.Arg != nil {
			t.Fatalf("expected null bulk, got %v", bulk.Arg)
		}
	} else {
		if bulk.Arg == nil {
			t.Fatalf("expected bulk %q, got null", expected)
		}
		if string(bulk.Arg) != string(expected) {
			t.Fatalf("expected bulk %q, got %q", expected, bulk.Arg)
		}
	}
}

// 断言是 MultiBulkReply 且内容等于 expected（支持 nil 元素）
func assertMultiBulkReply(t *testing.T, reply resp.Reply, expected [][]byte) {
	t.Helper()
	multi, ok := reply.(*resp.MultiBulkReply)
	if !ok {
		t.Fatalf("expected *resp.MultiBulkReply, got %T", reply)
	}
	if len(multi.Args) != len(expected) {
		t.Fatalf("multi bulk length mismatch: expected %d, got %d", len(expected), len(multi.Args))
	}
	for i, exp := range expected {
		got := multi.Args[i]
		if exp == nil {
			if got != nil {
				t.Fatalf("at index %d: expected nil, got %v", i, got)
			}
		} else {
			if got == nil {
				t.Fatalf("at index %d: expected %q, got nil", i, exp)
			}
			if string(got) != string(exp) {
				t.Fatalf("at index %d: expected %q, got %q", i, exp, got)
			}
		}
	}
}

// 断言是 StandardErrReply 且包含 substr
func assertErrorReply(t *testing.T, reply resp.Reply, substr string) {
	t.Helper()
	err, ok := reply.(*resp.StandardErrReply)
	if !ok {
		t.Fatalf("expected *resp.StandardErrReply, got %T", reply)
	}
	if err.Status != substr && !contains(err.Status, substr) {
		t.Fatalf("error reply does not contain '%s', got: '%s'", substr, err.Status)
	}
}

// 断言是 OK Reply
func assertOKReply(t *testing.T, reply resp.Reply) {
	t.Helper()
	okReply, ok := reply.(*resp.SimpleStringReply)
	if !ok {
		t.Fatalf("expected *resp.SimpleStringReply, got %T", reply)
	}
	if okReply.Status != "OK" {
		t.Fatalf("expected OK, got %s", okReply.Status)
	}
}

func isOKReply(t *testing.T, reply resp.Reply) bool {
	t.Helper()
	return strings.Contains(string(reply.ToBytes()), "OK")
}

func isNuAllBulk(t *testing.T, reply resp.Reply) bool {
	t.Helper()
	return reply == resp.NullBulkReply
}

func getMultiBulkValues(t *testing.T, reply resp.Reply) [][]byte {
	mulReply, ok := reply.(*resp.MultiBulkReply)
	if !ok {
		t.Fatalf("reply type is not MultiBulk")
	}
	return mulReply.Args
}

func getIntValue(t *testing.T, reply resp.Reply) int64 {
	intReply, ok := reply.(*resp.IntReply)
	if !ok {
		t.Fatalf("reply type is not intVal")
	}
	return intReply.IntVal
}

func getBulkValue(t *testing.T, reply resp.Reply) []byte {
	bultReply, ok := reply.(*resp.BulkReply)
	if !ok {
		t.Fatalf("reply type is not BulkValue")
	}
	return bultReply.Arg
}

func getErrorString(t *testing.T, reply resp.Reply) string {
	bytes := reply.ToBytes()
	if len(bytes) == 0 || bytes[0] != '-' {
		t.Fatalf("not an error reply: %q", string(bytes))
	}
	return string(bytes[1 : len(bytes)-2])
}

func isNullBulk(t *testing.T, reply resp.Reply) bool {
	return string(reply.ToBytes()) == "$-1\r\n"
}

// 断言函数
func assertEqualInt(t *testing.T, reply resp.Reply, expected int64) {
	if actual := getIntValue(t, reply); actual != expected {
		t.Errorf("expected %d, got %d", expected, actual)
	}
}

func assertEqualBulk(t *testing.T, reply resp.Reply, expected []byte) {
	actual := getBulkValue(t, reply)
	if expected == nil {
		if actual != nil {
			t.Errorf("expected null, got %q", actual)
		}
	} else {
		if actual == nil {
			t.Errorf("expected %q, got null", expected)
		} else if string(actual) != string(expected) {
			t.Errorf("expected %q, got %q", expected, actual)
		}
	}
}

func assertEqualMultiBulk(t *testing.T, reply resp.Reply, expected [][]byte) {
	actual := getMultiBulkValues(t, reply)
	if len(actual) != len(expected) {
		t.Fatalf("length mismatch: expected %d, got %d", len(expected), len(actual))
	}
	for i := range expected {
		if expected[i] == nil {
			if actual[i] != nil {
				t.Errorf("at %d: expected nil, got %q", i, actual[i])
			}
		} else {
			if actual[i] == nil {
				t.Errorf("at %d: expected %q, got nil", i, expected[i])
			} else if string(actual[i]) != string(expected[i]) {
				t.Errorf("at %d: expected %q, got %q", i, expected[i], actual[i])
			}
		}
	}
}

// 辅助：判断字符串是否包含子串
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
