// command_test.go
package command

import (
	"goredis/internal/data"
	"goredis/internal/types"
	"strings"
	"testing"
)

func TestExecHSet(t *testing.T) {
	db := NewMockDB()

	t.Run("normal insert", func(t *testing.T) {
		reply := execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})
		assertIntReply(t, reply, 1)
	})

	t.Run("update existing", func(t *testing.T) {
		execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v2")})
		reply := execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v3")})
		assertIntReply(t, reply, 0)
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execHSet(db, [][]byte{[]byte("k")})
		assertErrorReply(t, reply, "ERR wrong number of arguments for 'hset' command")
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "not hash"})
		reply := execHSet(db, [][]byte{[]byte("str"), []byte("f"), []byte("v")})
		assertErrorReply(t, reply, "WRONGTYPE Operation against a key holding the wrong kind of value")
	})
}

func TestExecHGet(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})

	t.Run("exists", func(t *testing.T) {
		reply := execHGet(db, [][]byte{[]byte("h"), []byte("f1")})
		assertBulkReply(t, reply, []byte("v1"))
	})

	t.Run("field not exists", func(t *testing.T) {
		reply := execHGet(db, [][]byte{[]byte("h"), []byte("missing")})
		assertBulkReply(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHGet(db, [][]byte{[]byte("nokey"), []byte("f")})
		assertBulkReply(t, reply, nil)
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execHGet(db, [][]byte{[]byte("k")})
		assertErrorReply(t, reply, "ERR wrong number of arguments for 'hget' command")
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("s", &types.DataEntity{Data: "string"})
		reply := execHGet(db, [][]byte{[]byte("s"), []byte("f")})
		assertErrorReply(t, reply, "WRONGTYPE")
	})
}

func TestExecHMGet(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})
	execHSet(db, [][]byte{[]byte("h"), []byte("f3"), []byte("v3")})

	t.Run("mixed fields", func(t *testing.T) {
		reply := execHMGet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("f2"), []byte("f3")})
		assertMultiBulkReply(t, reply, [][]byte{
			[]byte("v1"),
			nil,
			[]byte("v3"),
		})
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHMGet(db, [][]byte{[]byte("nokey"), []byte("f1"), []byte("f2")})
		assertMultiBulkReply(t, reply, [][]byte{nil, nil})
	})
}

func TestExecHMSet(t *testing.T) {
	db := NewMockDB()

	t.Run("normal", func(t *testing.T) {
		reply := execHMSet(db, [][]byte{
			[]byte("h"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2"),
		})
		assertOKReply(t, reply)

		// 验证写入
		entity, _ := db.GetEntity("h")
		h := entity.Data.(data.Hash)
		v1, _ := h.HGet("f1")
		v2, _ := h.HGet("f2")
		if string(v1) != "v1" || string(v2) != "v2" {
			t.Errorf("HMSET did not set values correctly")
		}
	})

	t.Run("odd args", func(t *testing.T) {
		reply := execHMSet(db, [][]byte{[]byte("h"), []byte("f1")})
		assertErrorReply(t, reply, "ERR wrong number of arguments for 'hmset' command")
	})
}

// 其他函数（HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL）测试方式类似，略...
func TestExecHDel(t *testing.T) {
	db := NewMockDB()
	// 先设置一个 hash
	execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})
	execHSet(db, [][]byte{[]byte("h"), []byte("f2"), []byte("v2")})
	execHSet(db, [][]byte{[]byte("h"), []byte("f3"), []byte("v3")})

	t.Run("delete one existing field", func(t *testing.T) {
		reply := execHDel(db, [][]byte{[]byte("h"), []byte("f1")})
		assertEqualInt(t, reply, 1)
	})

	t.Run("delete multiple fields (some missing)", func(t *testing.T) {
		reply := execHDel(db, [][]byte{[]byte("h"), []byte("f2"), []byte("f99"), []byte("f3")})
		assertEqualInt(t, reply, 2) // f2 和 f3 存在
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHDel(db, [][]byte{[]byte("nokey"), []byte("f")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong args (only key)", func(t *testing.T) {
		reply := execHDel(db, [][]byte{[]byte("h")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hdel' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "not a hash"})
		reply := execHDel(db, [][]byte{[]byte("str"), []byte("f")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecHExists(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})

	t.Run("field exists", func(t *testing.T) {
		reply := execHExists(db, [][]byte{[]byte("h"), []byte("f1")})
		assertEqualInt(t, reply, 1)
	})

	t.Run("field not exists", func(t *testing.T) {
		reply := execHExists(db, [][]byte{[]byte("h"), []byte("missing")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHExists(db, [][]byte{[]byte("nokey"), []byte("f")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong args (only key)", func(t *testing.T) {
		reply := execHExists(db, [][]byte{[]byte("h")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hexists' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "string"})
		reply := execHExists(db, [][]byte{[]byte("str"), []byte("f")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecHLEN(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("f1"), []byte("v1")})
	execHSet(db, [][]byte{[]byte("h"), []byte("f2"), []byte("v2")})

	t.Run("normal", func(t *testing.T) {
		reply := execHLEN(db, [][]byte{[]byte("h")})
		assertEqualInt(t, reply, 2)
	})

	t.Run("empty hash (key not exists)", func(t *testing.T) {
		reply := execHLEN(db, [][]byte{[]byte("nokey")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong args (extra arg)", func(t *testing.T) {
		reply := execHLEN(db, [][]byte{[]byte("h"), []byte("extra")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hlen' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("not hash")})
		reply := execHLEN(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecHKeys(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("name"), []byte("redis")})
	execHSet(db, [][]byte{[]byte("h"), []byte("version"), []byte("7.0")})

	t.Run("normal", func(t *testing.T) {
		reply := execHKeys(db, [][]byte{[]byte("h")})
		keys := getMultiBulkValues(t, reply)
		// 转为 set 比较（顺序不保证）
		keyMap := make(map[string]bool)
		for _, k := range keys {
			keyMap[string(k)] = true
		}
		if !keyMap["name"] || !keyMap["version"] || len(keyMap) != 2 {
			t.Errorf("unexpected keys: %v", keys)
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHKeys(db, [][]byte{[]byte("nokey")})
		if !isNullBulk(t, reply) {
			t.Errorf("expected null bulk for non-existing key")
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execHKeys(db, [][]byte{[]byte("h"), []byte("extra")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hkeys' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("set", &types.DataEntity{Data: map[string]struct{}{"a": {}}})
		reply := execHKeys(db, [][]byte{[]byte("set")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecHVals(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("name"), []byte("redis")})
	execHSet(db, [][]byte{[]byte("h"), []byte("version"), []byte("7.0")})

	t.Run("normal", func(t *testing.T) {
		reply := execHVals(db, [][]byte{[]byte("h")})
		vals := getMultiBulkValues(t, reply)
		// 转为 set 比较（顺序不保证）
		valMap := make(map[string]bool)
		for _, v := range vals {
			valMap[string(v)] = true
		}
		if !valMap["redis"] || !valMap["7.0"] || len(valMap) != 2 {
			t.Errorf("unexpected values: %v", vals)
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHVals(db, [][]byte{[]byte("nokey")})
		if !isNuAllBulk(t, reply) {
			t.Errorf("expected null bulk for non-existing key")
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execHVals(db, [][]byte{[]byte("h"), []byte("extra")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hvals' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("zset", &types.DataEntity{Data: []interface{}{}})
		reply := execHVals(db, [][]byte{[]byte("zset")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecHGetAll(t *testing.T) {
	db := NewMockDB()
	execHSet(db, [][]byte{[]byte("h"), []byte("name"), []byte("redis")})
	execHSet(db, [][]byte{[]byte("h"), []byte("version"), []byte("7.0")})

	t.Run("normal", func(t *testing.T) {
		reply := execHGetAll(db, [][]byte{[]byte("h")})
		pairs := getMultiBulkValues(t, reply)
		if len(pairs)%2 != 0 {
			t.Fatalf("hgetall returned odd number of elements: %d", len(pairs))
		}
		result := make(map[string][]byte)
		for i := 0; i < len(pairs); i += 2 {
			key := string(pairs[i])
			val := pairs[i+1]
			result[key] = val
		}
		if string(result["name"]) != "redis" || string(result["version"]) != "7.0" {
			t.Errorf("unexpected hgetall result: %v", result)
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execHGetAll(db, [][]byte{[]byte("nokey")})
		if !isNullBulk(t, reply) {
			t.Errorf("expected null bulk for non-existing key")
		}
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execHGetAll(db, [][]byte{[]byte("h"), []byte("extra")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "wrong number of arguments for 'hgetall' command") {
			t.Errorf("unexpected error: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execHGetAll(db, [][]byte{[]byte("str")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}
