package command

import (
	"goredis/internal/resp"
	"goredis/internal/types"
	"testing"
	"time"
)

func TestExecDel(t *testing.T) {
	db := NewMockDB()

	// 准备数据
	db.PutEntity("k1", &types.DataEntity{Data: "v1"})
	db.PutEntity("k2", &types.DataEntity{Data: "v2"})
	db.PutEntity("k3", &types.DataEntity{Data: "v3"})

	t.Run("delete multiple existing keys", func(t *testing.T) {
		reply := execDel(db, [][]byte{[]byte("k1"), []byte("k2")})
		assertEqualInt(t, reply, 2)

		// 验证已删除
		if _, exists := db.GetEntity("k1"); exists {
			t.Error("k1 should be deleted")
		}
		if _, exists := db.GetEntity("k2"); exists {
			t.Error("k2 should be deleted")
		}
		if _, exists := db.GetEntity("k3"); !exists {
			t.Error("k3 should still exist")
		}
	})

	t.Run("delete non-existing keys", func(t *testing.T) {
		reply := execDel(db, [][]byte{[]byte("k99"), []byte("k100")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("mixed existing and non-existing", func(t *testing.T) {
		db.PutEntity("k4", &types.DataEntity{Data: "v4"})
		reply := execDel(db, [][]byte{[]byte("k3"), []byte("k99"), []byte("k4")})
		assertEqualInt(t, reply, 2)
	})
}

func TestExecExpire(t *testing.T) {
	db := NewMockDB()
	db.PutEntity("mykey", &types.DataEntity{Data: "value"})

	t.Run("normal expire", func(t *testing.T) {
		reply := execExpire(db, [][]byte{[]byte("mykey"), []byte("10")})
		assertEqualInt(t, reply, 1)

		// 验证 TTL 已设置
		if expireTime, ok := db.GetExpireTime("mykey"); !ok {
			t.Error("expire time not set")
		} else {
			now := time.Now()
			if expireTime.Before(now) || expireTime.After(now.Add(11*time.Second)) {
				t.Errorf("unexpected expire time: %v", expireTime)
			}
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execExpire(db, [][]byte{[]byte("nokey"), []byte("5")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("invalid expire time (non-integer)", func(t *testing.T) {
		reply := execExpire(db, [][]byte{[]byte("mykey"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR invalid expire time" {
			t.Errorf("expected 'ERR invalid expire time', got: %s", errMsg)
		}
	})

	t.Run("expire <= 0 should delete key", func(t *testing.T) {
		db.PutEntity("temp", &types.DataEntity{Data: "to-delete"})
		reply := execExpire(db, [][]byte{[]byte("temp"), []byte("0")})
		assertEqualInt(t, reply, 1)

		if _, exists := db.GetEntity("temp"); exists {
			t.Error("key with expire <=0 should be deleted immediately")
		}
	})

	t.Run("negative expire", func(t *testing.T) {
		db.PutEntity("neg", &types.DataEntity{Data: "neg"})
		reply := execExpire(db, [][]byte{[]byte("neg"), []byte("-5")})
		assertEqualInt(t, reply, 1)

		if _, exists := db.GetEntity("neg"); exists {
			t.Error("key with negative expire should be deleted")
		}
	})
}

// 测试命令注册表（可选，但推荐）
func TestRegisterCommand(t *testing.T) {
	// 清理全局状态（注意：并发不安全，仅用于测试）
	originalTable := make(map[string]*Command)
	for k, v := range cmdTable {
		originalTable[k] = v
	}
	defer func() {
		cmdTable = originalTable
	}()

	cmd := &Command{
		Name: "TESTCMD",
		Executor: func(db types.Database, args [][]byte) resp.Reply {
			return resp.MakeOkReply()
		},
		Arity: 2,
	}

	RegisterCommand(cmd)

	registered, ok := cmdTable["TESTCMD"]
	if !ok {
		t.Fatal("command not registered")
	}
	if registered.Name != "TESTCMD" || registered.Arity != 2 {
		t.Errorf("registered command mismatch: %+v", registered)
	}

	// 执行验证
	db := NewMockDB()
	reply := registered.Executor(db, [][]byte{[]byte("arg1")})
	if !isOKReply(t, reply) {
		t.Error("executor did not return OK")
	}
}
