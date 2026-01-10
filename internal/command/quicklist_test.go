package command

import (
	"goredis/internal/types"
	"testing"
)

func TestExecLPush(t *testing.T) {
	db := NewMockDB()

	t.Run("push multiple values", func(t *testing.T) {
		reply := execLPush(db, [][]byte{
			[]byte("mylist"), []byte("v1"), []byte("v2"), []byte("v3"),
		})
		assertEqualInt(t, reply, 3)

		// 验证顺序：LPOP 应该得到 v3
		popReply := execLPop(db, [][]byte{[]byte("mylist")})
		assertEqualBulk(t, popReply, []byte("v3"))
	})

	t.Run("push to existing list", func(t *testing.T) {
		execLPush(db, [][]byte{[]byte("list2"), []byte("a")})
		reply := execLPush(db, [][]byte{[]byte("list2"), []byte("b")})
		assertEqualInt(t, reply, 2)

		// LPOP: b, then a
		assertEqualBulk(t, execLPop(db, [][]byte{[]byte("list2")}), []byte("b"))
		assertEqualBulk(t, execLPop(db, [][]byte{[]byte("list2")}), []byte("a"))
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "not a list"})
		reply := execLPush(db, [][]byte{[]byte("str"), []byte("v")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecRPush(t *testing.T) {
	db := NewMockDB()

	t.Run("push multiple values", func(t *testing.T) {
		reply := execRPush(db, [][]byte{
			[]byte("mylist"), []byte("v1"), []byte("v2"), []byte("v3"),
		})
		assertEqualInt(t, reply, 3)

		// RPOP 应该得到 v3
		popReply := execRPop(db, [][]byte{[]byte("mylist")})
		assertEqualBulk(t, popReply, []byte("v3"))
	})

	t.Run("push to existing list", func(t *testing.T) {
		execRPush(db, [][]byte{[]byte("list2"), []byte("a")})
		reply := execRPush(db, [][]byte{[]byte("list2"), []byte("b")})
		assertEqualInt(t, reply, 2)

		// LPOP: a, then b
		assertEqualBulk(t, execLPop(db, [][]byte{[]byte("list2")}), []byte("a"))
		assertEqualBulk(t, execLPop(db, [][]byte{[]byte("list2")}), []byte("b"))
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execRPush(db, [][]byte{[]byte("hash"), []byte("v")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLPop(t *testing.T) {
	db := NewMockDB()
	execLPush(db, [][]byte{[]byte("l"), []byte("v3"), []byte("v2"), []byte("v1")}) // list: [v1, v2, v3]

	t.Run("pop from non-empty list", func(t *testing.T) {
		reply := execLPop(db, [][]byte{[]byte("l")})
		assertEqualBulk(t, reply, []byte("v1"))
	})

	t.Run("pop from empty list (after all popped)", func(t *testing.T) {
		execLPop(db, [][]byte{[]byte("l")}) // v2
		execLPop(db, [][]byte{[]byte("l")}) // v3
		reply := execLPop(db, [][]byte{[]byte("l")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLPop(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("set", &types.DataEntity{Data: []string{"a"}})
		reply := execLPop(db, [][]byte{[]byte("set")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecRPop(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("v1"), []byte("v2"), []byte("v3")}) // list: [v1, v2, v3]

	t.Run("pop from non-empty list", func(t *testing.T) {
		reply := execRPop(db, [][]byte{[]byte("l")})
		assertEqualBulk(t, reply, []byte("v3"))
	})

	t.Run("pop until empty", func(t *testing.T) {
		execRPop(db, [][]byte{[]byte("l")}) // v2
		execRPop(db, [][]byte{[]byte("l")}) // v1
		reply := execRPop(db, [][]byte{[]byte("l")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execRPop(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("int", &types.DataEntity{Data: int64(42)})
		reply := execRPop(db, [][]byte{[]byte("int")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLLen(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b")})

	t.Run("normal", func(t *testing.T) {
		reply := execLLen(db, [][]byte{[]byte("l")})
		assertEqualInt(t, reply, 2)
	})

	t.Run("empty key", func(t *testing.T) {
		reply := execLLen(db, [][]byte{[]byte("nokey")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execLLen(db, [][]byte{[]byte("str")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLIndex(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b"), []byte("c")}) // [a, b, c]

	t.Run("valid index", func(t *testing.T) {
		reply := execLIndex(db, [][]byte{[]byte("l"), []byte("1")})
		assertEqualBulk(t, reply, []byte("b"))
	})

	t.Run("negative index", func(t *testing.T) {
		reply := execLIndex(db, [][]byte{[]byte("l"), []byte("-1")})
		assertEqualBulk(t, reply, []byte("c")) // last element
	})

	t.Run("out of range", func(t *testing.T) {
		reply := execLIndex(db, [][]byte{[]byte("l"), []byte("10")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLIndex(db, [][]byte{[]byte("nokey"), []byte("0")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("invalid index format", func(t *testing.T) {
		reply := execLIndex(db, [][]byte{[]byte("l"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execLIndex(db, [][]byte{[]byte("hash"), []byte("0")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLSet(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b"), []byte("c")})

	t.Run("valid set", func(t *testing.T) {
		reply := execLSet(db, [][]byte{[]byte("l"), []byte("1"), []byte("B")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}

		assertEqualBulk(t, execLIndex(db, [][]byte{[]byte("l"), []byte("1")}), []byte("B"))
	})

	t.Run("out of range", func(t *testing.T) {
		reply := execLSet(db, [][]byte{[]byte("l"), []byte("10"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR index out of range" {
			t.Errorf("expected out of range, got: %s", errMsg)
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLSet(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR no such key" {
			t.Errorf("expected 'no such key', got: %s", errMsg)
		}
	})

	t.Run("invalid index", func(t *testing.T) {
		reply := execLSet(db, [][]byte{[]byte("l"), []byte("abc"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execLSet(db, [][]byte{[]byte("str"), []byte("0"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLRange(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b"), []byte("c"), []byte("d")}) // [a,b,c,d]

	t.Run("full range", func(t *testing.T) {
		reply := execLRange(db, [][]byte{[]byte("l"), []byte("0"), []byte("-1")})
		assertEqualMultiBulk(t, reply, [][]byte{
			[]byte("a"), []byte("b"), []byte("c"), []byte("d"),
		})
	})

	t.Run("partial range", func(t *testing.T) {
		reply := execLRange(db, [][]byte{[]byte("l"), []byte("1"), []byte("2")})
		assertEqualMultiBulk(t, reply, [][]byte{[]byte("b"), []byte("c")})
	})

	t.Run("negative indices", func(t *testing.T) {
		reply := execLRange(db, [][]byte{[]byte("l"), []byte("-2"), []byte("-1")})
		assertEqualMultiBulk(t, reply, [][]byte{[]byte("c"), []byte("d")})
	})

	t.Run("empty range", func(t *testing.T) {
		reply := execLRange(db, [][]byte{[]byte("l"), []byte("10"), []byte("20")})
		assertEqualMultiBulk(t, reply, [][]byte{})
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLRange(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("1")})
		assertEqualBulk(t, reply, nil) // 注意：LRange 返回 Null Bulk when key not exists
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("set", &types.DataEntity{Data: []string{"x"}})
		reply := execLRange(db, [][]byte{[]byte("set"), []byte("0"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLRem(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b"), []byte("a"), []byte("c"), []byte("a")}) // [a,b,a,c,a]

	t.Run("remove all occurrences (count=0)", func(t *testing.T) {
		reply := execLRem(db, [][]byte{[]byte("l"), []byte("0"), []byte("a")})
		assertEqualInt(t, reply, 3)

		// Remaining: [b, c]
		assertEqualMultiBulk(t, execLRange(db, [][]byte{[]byte("l"), []byte("0"), []byte("-1")}), [][]byte{
			[]byte("b"), []byte("c"),
		})
	})

	t.Run("remove first 2 occurrences (count=2)", func(t *testing.T) {
		// Reset list
		db = NewMockDB()
		execRPush(db, [][]byte{[]byte("l2"), []byte("x"), []byte("y"), []byte("x"), []byte("z"), []byte("x")})
		reply := execLRem(db, [][]byte{[]byte("l2"), []byte("2"), []byte("x")})
		assertEqualInt(t, reply, 2)

		// Remaining: [y, z, x]
		assertEqualMultiBulk(t, execLRange(db, [][]byte{[]byte("l2"), []byte("0"), []byte("-1")}), [][]byte{
			[]byte("y"), []byte("z"), []byte("x"),
		})
	})

	t.Run("remove last 1 occurrence (count=-1)", func(t *testing.T) {
		db = NewMockDB()
		execRPush(db, [][]byte{[]byte("l3"), []byte("p"), []byte("q"), []byte("p"), []byte("r"), []byte("p")})
		reply := execLRem(db, [][]byte{[]byte("l3"), []byte("-1"), []byte("p")})
		assertEqualInt(t, reply, 1)

		// Remaining: [p, q, p, r]
		assertEqualMultiBulk(t, execLRange(db, [][]byte{[]byte("l3"), []byte("0"), []byte("-1")}), [][]byte{
			[]byte("p"), []byte("q"), []byte("p"), []byte("r"),
		})
	})

	t.Run("value not found", func(t *testing.T) {
		reply := execLRem(db, [][]byte{[]byte("l3"), []byte("0"), []byte("notfound")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLRem(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("x")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("invalid count", func(t *testing.T) {
		reply := execLRem(db, [][]byte{[]byte("l"), []byte("abc"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("int", &types.DataEntity{Data: int64(1)})
		reply := execLRem(db, [][]byte{[]byte("int"), []byte("0"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}

func TestExecLTrim(t *testing.T) {
	db := NewMockDB()
	execRPush(db, [][]byte{[]byte("l"), []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}) // [a,b,c,d,e]

	t.Run("normal trim", func(t *testing.T) {
		reply := execLTrim(db, [][]byte{[]byte("l"), []byte("1"), []byte("3")})
		if !isOKReply(t, reply) {
			t.Error("expected OK")
		}

		assertEqualMultiBulk(t, execLRange(db, [][]byte{[]byte("l"), []byte("0"), []byte("-1")}), [][]byte{
			[]byte("b"), []byte("c"), []byte("d"),
		})
	})

	t.Run("trim with negative indices", func(t *testing.T) {
		db = NewMockDB()
		execRPush(db, [][]byte{[]byte("l2"), []byte("u"), []byte("v"), []byte("w"), []byte("x")})
		execLTrim(db, [][]byte{[]byte("l2"), []byte("-2"), []byte("-1")})
		assertEqualMultiBulk(t, execLRange(db, [][]byte{[]byte("l2"), []byte("0"), []byte("-1")}), [][]byte{
			[]byte("w"), []byte("x"),
		})
	})

	t.Run("trim to empty", func(t *testing.T) {
		execLTrim(db, [][]byte{[]byte("l"), []byte("10"), []byte("20")})
		assertEqualInt(t, execLLen(db, [][]byte{[]byte("l")}), 0)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execLTrim(db, [][]byte{[]byte("nokey"), []byte("0"), []byte("1")})
		if !isOKReply(t, reply) {
			t.Error("expected OK even if key not exists")
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execLTrim(db, [][]byte{[]byte("hash"), []byte("0"), []byte("1")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR wrong type" {
			t.Errorf("expected 'ERR wrong type', got: %s", errMsg)
		}
	})
}
