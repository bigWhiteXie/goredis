package command

import (
	"goredis/internal/types"
	"testing"
	"time"
)

func TestExecIncr(t *testing.T) {
	db := NewMockDB()

	t.Run("incr on non-existing key", func(t *testing.T) {
		reply := execIncr(db, [][]byte{[]byte("counter")})
		assertEqualInt(t, reply, 1)

		// Verify stored value
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("counter")}), []byte("1"))
	})

	t.Run("incr on existing integer", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("num"), []byte("10")})
		reply := execIncr(db, [][]byte{[]byte("num")})
		assertEqualInt(t, reply, 11)
	})

	t.Run("incr on non-integer string", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("str"), []byte("hello")})
		reply := execIncr(db, [][]byte{[]byte("str")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("not a string")})
		reply := execIncr(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecDecr(t *testing.T) {
	db := NewMockDB()

	t.Run("decr on non-existing key", func(t *testing.T) {
		reply := execDecr(db, [][]byte{[]byte("counter")})
		assertEqualInt(t, reply, -1)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("counter")}), []byte("-1"))
	})

	t.Run("decr on existing integer", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("num"), []byte("5")})
		reply := execDecr(db, [][]byte{[]byte("num")})
		assertEqualInt(t, reply, 4)
	})

	t.Run("decr on non-integer", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("str"), []byte("world")})
		reply := execDecr(db, [][]byte{[]byte("str")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})
}

func TestExecIncrBy(t *testing.T) {
	db := NewMockDB()

	t.Run("positive delta", func(t *testing.T) {
		reply := execIncrBy(db, [][]byte{[]byte("k"), []byte("10")})
		assertEqualInt(t, reply, 10)
	})

	t.Run("negative delta", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("5")})
		reply := execIncrBy(db, [][]byte{[]byte("k"), []byte("-3")})
		assertEqualInt(t, reply, 2)
	})

	t.Run("invalid delta", func(t *testing.T) {
		reply := execIncrBy(db, [][]byte{[]byte("k"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer or out of range" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})
}

func TestExecAppend(t *testing.T) {
	db := NewMockDB()

	t.Run("append to non-existing key", func(t *testing.T) {
		reply := execAppend(db, [][]byte{[]byte("k"), []byte("world")})
		assertEqualInt(t, reply, 5)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("world"))
	})

	t.Run("append to existing string", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("hello")})
		reply := execAppend(db, [][]byte{[]byte("k"), []byte(" world")})
		assertEqualInt(t, reply, 11)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("hello world"))
	})

	t.Run("append to integer (should convert to raw)", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("num"), []byte("123")})
		reply := execAppend(db, [][]byte{[]byte("num"), []byte("abc")})
		assertEqualInt(t, reply, 6)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("num")}), []byte("123abc"))
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("set", &types.DataEntity{Data: map[string]struct{}{}})
		reply := execAppend(db, [][]byte{[]byte("set"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecStrLen(t *testing.T) {
	db := NewMockDB()

	t.Run("existing string", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("hello")})
		reply := execStrLen(db, [][]byte{[]byte("k")})
		assertEqualInt(t, reply, 5)
	})

	t.Run("non-existing key", func(t *testing.T) {
		reply := execStrLen(db, [][]byte{[]byte("nokey")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []interface{}{}})
		reply := execStrLen(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecSetNX(t *testing.T) {
	db := NewMockDB()

	t.Run("set on non-existing key", func(t *testing.T) {
		reply := execSetNX(db, [][]byte{[]byte("k"), []byte("v")})
		assertEqualInt(t, reply, 1)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("v"))
	})

	t.Run("set on existing key", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k2"), []byte("old")})
		reply := execSetNX(db, [][]byte{[]byte("k2"), []byte("new")})
		assertEqualInt(t, reply, 0)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k2")}), []byte("old")) // unchanged
	})

	t.Run("wrong type should not happen (NX only checks existence)", func(t *testing.T) {
		// Even if key exists as wrong type, NX returns 0
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execSetNX(db, [][]byte{[]byte("hash"), []byte("v")})
		assertEqualInt(t, reply, 0)
	})
}

func TestExecDecrBy(t *testing.T) {
	db := NewMockDB()

	t.Run("decr by 5 on non-existing key", func(t *testing.T) {
		reply := execDecrBy(db, [][]byte{[]byte("k"), []byte("5")})
		assertEqualInt(t, reply, -5)
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("-5"))
	})

	t.Run("decr by 2 on existing", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("10")})
		reply := execDecrBy(db, [][]byte{[]byte("k"), []byte("2")})
		assertEqualInt(t, reply, 8)
	})

	t.Run("invalid delta", func(t *testing.T) {
		reply := execDecrBy(db, [][]byte{[]byte("k"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer or out of range" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})
}

func TestExecSet(t *testing.T) {
	db := NewMockDB()

	t.Run("simple set", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k"), []byte("v")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("v"))
	})

	t.Run("set with EX", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("10")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("v"))

		// Verify TTL set
		if expireTime, ok := db.GetExpireTime("k"); !ok {
			t.Error("TTL not set")
		} else {
			now := time.Now()
			if expireTime.Before(now) || expireTime.After(now.Add(11*time.Second)) {
				t.Errorf("unexpected expire time: %v", expireTime)
			}
		}
	})

	t.Run("set with NX on non-existing", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("non-existing"), []byte("v"), []byte("NX")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("non-existing")}), []byte("v"))
	})

	t.Run("set with NX on existing", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k2"), []byte("old")})
		reply := execSet(db, [][]byte{[]byte("k2"), []byte("new"), []byte("NX")})
		if !isNullBulk(t, reply) {
			t.Error("expected Null Bulk for NX on existing key")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k2")}), []byte("old")) // unchanged
	})

	t.Run("set with EX and NX", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k3"), []byte("v"), []byte("EX"), []byte("5"), []byte("NX")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k3")}), []byte("v"))
	})

	t.Run("invalid EX value", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR invalid expire time" {
			t.Errorf("expected invalid expire time, got: %s", errMsg)
		}
	})

	t.Run("unknown option", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k"), []byte("v"), []byte("XX")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR unknown option" {
			t.Errorf("expected unknown option, got: %s", errMsg)
		}
	})

	t.Run("wrong number of args for EX", func(t *testing.T) {
		reply := execSet(db, [][]byte{[]byte("k"), []byte("v"), []byte("EX")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong number of arguments for 'set' command" {
			t.Errorf("expected arg num error, got: %s", errMsg)
		}
	})
}

func TestExecGet(t *testing.T) {
	db := NewMockDB()

	t.Run("existing key", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("value")})
		reply := execGet(db, [][]byte{[]byte("k")})
		assertEqualBulk(t, reply, []byte("value"))
	})

	t.Run("non-existing key", func(t *testing.T) {
		reply := execGet(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("list")})
		reply := execGet(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecMSet(t *testing.T) {
	db := NewMockDB()

	t.Run("normal", func(t *testing.T) {
		reply := execMSet(db, [][]byte{
			[]byte("k1"), []byte("v1"),
			[]byte("k2"), []byte("v2"),
		})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k1")}), []byte("v1"))
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k2")}), []byte("v2"))
	})

	t.Run("odd number of args", func(t *testing.T) {
		reply := execMSet(db, [][]byte{[]byte("k")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong number of arguments for 'mset' command" {
			t.Errorf("expected arg num error, got: %s", errMsg)
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		execSet(db, [][]byte{[]byte("k"), []byte("old")})
		execMSet(db, [][]byte{[]byte("k"), []byte("new")})
		assertEqualBulk(t, execGet(db, [][]byte{[]byte("k")}), []byte("new"))
	})
}

func TestExecMGet(t *testing.T) {
	db := NewMockDB()
	execSet(db, [][]byte{[]byte("k1"), []byte("v1")})
	execSet(db, [][]byte{[]byte("k3"), []byte("v3")})

	t.Run("mixed existing and non-existing", func(t *testing.T) {
		reply := execMGet(db, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")})
		expected := [][]byte{
			[]byte("v1"),
			nil,
			[]byte("v3"),
		}
		assertEqualMultiBulk(t, reply, expected)
	})

	t.Run("all non-existing", func(t *testing.T) {
		reply := execMGet(db, [][]byte{[]byte("x"), []byte("y")})
		expected := [][]byte{nil, nil}
		assertEqualMultiBulk(t, reply, expected)
	})

	t.Run("wrong type treated as nil", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []interface{}{}})
		reply := execMGet(db, [][]byte{[]byte("k1"), []byte("list")})
		expected := [][]byte{
			[]byte("v1"),
			nil, // wrong type → nil
		}
		assertEqualMultiBulk(t, reply, expected)
	})
}
