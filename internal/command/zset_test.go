package command

import (
	"goredis/internal/types"
	"testing"
)

func TestExecZAdd(t *testing.T) {
	db := NewMockDB()

	t.Run("normal add", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{
			[]byte("z"), []byte("1.5"), []byte("a"), []byte("2.5"), []byte("b"),
		})
		assertEqualInt(t, reply, 2)

		// Verify scores
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("a")}), []byte("1.5"))
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("b")}), []byte("2.5"))
	})

	t.Run("update existing member", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{[]byte("z"), []byte("3.0"), []byte("a")})
		assertEqualInt(t, reply, 0) // no new member added
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("a")}), []byte("3"))
	})

	t.Run("NX option (only add new)", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{[]byte("z"), []byte("nx"), []byte("4.0"), []byte("c")})
		assertEqualInt(t, reply, 1)
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("c")}), []byte("4"))

		// Try to update existing with NX → should not update
		reply = execZAdd(db, [][]byte{[]byte("z"), []byte("nx"), []byte("5.0"), []byte("c")})
		assertEqualInt(t, reply, 0)
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("c")}), []byte("4")) // unchanged
	})

	t.Run("XX option (only update existing)", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{[]byte("z"), []byte("xx"), []byte("6.0"), []byte("d")})
		assertEqualInt(t, reply, 0) // d doesn't exist
		execZAdd(db, [][]byte{[]byte("z"), []byte("xx"), []byte("5.0"), []byte("c")})
		reply = execZAdd(db, [][]byte{[]byte("z"), []byte("xx"), []byte("7.0"), []byte("c")})
		assertEqualInt(t, reply, 0) // c exists
		assertEqualBulk(t, execZScore(db, [][]byte{[]byte("z"), []byte("c")}), []byte("7"))
	})

	t.Run("invalid score", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{[]byte("z2"), []byte("abc"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not a valid float" {
			t.Errorf("expected float error, got: %s", errMsg)
		}
	})

	t.Run("wrong args (odd number)", func(t *testing.T) {
		reply := execZAdd(db, [][]byte{[]byte("z"), []byte("1.0")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong number of arguments for 'zadd' command" {
			t.Errorf("expected arg num error, got: %s", errMsg)
		}
	})
}

func TestExecZCard(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b")})

	t.Run("normal", func(t *testing.T) {
		reply := execZCard(db, [][]byte{[]byte("z")})
		assertEqualInt(t, reply, 2)
	})

	t.Run("empty key", func(t *testing.T) {
		reply := execZCard(db, [][]byte{[]byte("nokey")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []interface{}{}})
		reply := execZCard(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZScore(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("3.14"), []byte("pi")})

	t.Run("member exists", func(t *testing.T) {
		reply := execZScore(db, [][]byte{[]byte("z"), []byte("pi")})
		assertEqualBulk(t, reply, []byte("3.14"))
	})

	t.Run("member not exists", func(t *testing.T) {
		reply := execZScore(db, [][]byte{[]byte("z"), []byte("e")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZScore(db, [][]byte{[]byte("nokey"), []byte("x")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execZScore(db, [][]byte{[]byte("hash"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZRank(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")})

	t.Run("member exists", func(t *testing.T) {
		reply := execZRank(db, [][]byte{[]byte("z"), []byte("b")})
		assertEqualInt(t, reply, 1) // 0-based rank
	})

	t.Run("member not exists", func(t *testing.T) {
		reply := execZRank(db, [][]byte{[]byte("z"), []byte("x")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZRank(db, [][]byte{[]byte("nokey"), []byte("a")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execZRank(db, [][]byte{[]byte("str"), []byte("a")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZRevRank(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")})

	t.Run("member exists", func(t *testing.T) {
		reply := execZRevRank(db, [][]byte{[]byte("z"), []byte("b")})
		assertEqualInt(t, reply, 1) // rev rank: c=0, b=1, a=2
	})

	t.Run("member not exists", func(t *testing.T) {
		reply := execZRevRank(db, [][]byte{[]byte("z"), []byte("x")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZRevRank(db, [][]byte{[]byte("nokey"), []byte("a")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("list")})
		reply := execZRevRank(db, [][]byte{[]byte("list"), []byte("a")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZRange(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")})

	t.Run("normal range", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("z"), []byte("0"), []byte("1")})
		assertEqualMultiBulk(t, reply, [][]byte{[]byte("a"), []byte("b")})
	})

	t.Run("withscores", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("z"), []byte("0"), []byte("1"), []byte("WITHSCORES")})
		assertEqualMultiBulk(t, reply, [][]byte{
			[]byte("a"), []byte("1"),
			[]byte("b"), []byte("2"),
		})
	})

	t.Run("negative indices", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("z"), []byte("-2"), []byte("-1")})
		assertEqualMultiBulk(t, reply, [][]byte{[]byte("b"), []byte("c")})
	})

	t.Run("out of range", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("z"), []byte("10"), []byte("20")})
		assertEqualMultiBulk(t, reply, [][]byte{})
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("1")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("invalid index", func(t *testing.T) {
		reply := execZRange(db, [][]byte{[]byte("z"), []byte("abc"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("int", &types.DataEntity{Data: int64(42)})
		reply := execZRange(db, [][]byte{[]byte("int"), []byte("0"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZRevRange(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")})

	t.Run("normal rev range", func(t *testing.T) {
		reply := execZRevRange(db, [][]byte{[]byte("z"), []byte("0"), []byte("1")})
		assertEqualMultiBulk(t, reply, [][]byte{[]byte("c"), []byte("b")})
	})

	t.Run("withscores", func(t *testing.T) {
		reply := execZRevRange(db, [][]byte{[]byte("z"), []byte("0"), []byte("1"), []byte("WITHSCORES")})
		assertEqualMultiBulk(t, reply, [][]byte{
			[]byte("c"), []byte("3"),
			[]byte("b"), []byte("2"),
		})
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZRevRange(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("1")})
		assertEqualBulk(t, reply, nil)
	})
}

func TestExecZCount(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1.0"), []byte("a"), []byte("2.5"), []byte("b"), []byte("3.0"), []byte("c")})

	t.Run("normal count", func(t *testing.T) {
		reply := execZCount(db, [][]byte{[]byte("z"), []byte("2.0"), []byte("3.0")})
		assertEqualInt(t, reply, 2) // b(2.5), c(3.0)
	})

	t.Run("no members in range", func(t *testing.T) {
		reply := execZCount(db, [][]byte{[]byte("z"), []byte("10"), []byte("20")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZCount(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("1")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("invalid min/max", func(t *testing.T) {
		reply := execZCount(db, [][]byte{[]byte("z"), []byte("abc"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not a float" {
			t.Errorf("expected float error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("set", &types.DataEntity{Data: map[string]struct{}{}})
		reply := execZCount(db, [][]byte{[]byte("set"), []byte("0"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecZRem(t *testing.T) {
	db := NewMockDB()
	execZAdd(db, [][]byte{[]byte("z"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")})

	t.Run("remove existing members", func(t *testing.T) {
		reply := execZRem(db, [][]byte{[]byte("z"), []byte("a"), []byte("x"), []byte("b")})
		assertEqualInt(t, reply, 2) // a and b removed
		assertEqualInt(t, execZCard(db, [][]byte{[]byte("z")}), 1)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execZRem(db, [][]byte{[]byte("nokey"), []byte("x")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong args", func(t *testing.T) {
		reply := execZRem(db, [][]byte{[]byte("z")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong number of arguments for 'zrem' command" {
			t.Errorf("expected arg num error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execZRem(db, [][]byte{[]byte("str"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}
