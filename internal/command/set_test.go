package command

import (
	"goredis/internal/resp"
	"goredis/internal/types"
	"strings"
	"testing"
)

func TestExecSAdd(t *testing.T) {
	db := NewMockDB()

	t.Run("add new members", func(t *testing.T) {
		reply := execSAdd(db, [][]byte{
			[]byte("myset"), []byte("a"), []byte("b"), []byte("c"),
		})
		assertEqualInt(t, reply, 3)
	})

	t.Run("add duplicates", func(t *testing.T) {
		reply := execSAdd(db, [][]byte{
			[]byte("myset"), []byte("a"), []byte("d"),
		})
		assertEqualInt(t, reply, 1) // only "d" is new
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("not a set")})
		reply := execSAdd(db, [][]byte{[]byte("list"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSRem(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("a"), []byte("b"), []byte("c")})

	t.Run("remove existing members", func(t *testing.T) {
		reply := execSRem(db, [][]byte{[]byte("s"), []byte("a"), []byte("x"), []byte("b")})
		assertEqualInt(t, reply, 2) // a and b removed
	})

	t.Run("remove from empty set (auto delete)", func(t *testing.T) {
		execSAdd(db, [][]byte{[]byte("temp"), []byte("only")})
		execSRem(db, [][]byte{[]byte("temp"), []byte("only")})
		// Key should be removed
		if _, exists := db.GetEntity("temp"); exists {
			t.Error("key should be deleted when set becomes empty")
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execSRem(db, [][]byte{[]byte("nokey"), []byte("x")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execSRem(db, [][]byte{[]byte("str"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSIsMember(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("hello"), []byte("world")})

	t.Run("member exists", func(t *testing.T) {
		reply := execSIsMember(db, [][]byte{[]byte("s"), []byte("hello")})
		assertEqualInt(t, reply, 1)
	})

	t.Run("member not exists", func(t *testing.T) {
		reply := execSIsMember(db, [][]byte{[]byte("s"), []byte("missing")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execSIsMember(db, [][]byte{[]byte("nokey"), []byte("x")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execSIsMember(db, [][]byte{[]byte("hash"), []byte("x")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSCard(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("a"), []byte("b")})

	t.Run("normal", func(t *testing.T) {
		reply := execSCard(db, [][]byte{[]byte("s")})
		assertEqualInt(t, reply, 2)
	})

	t.Run("empty key", func(t *testing.T) {
		reply := execSCard(db, [][]byte{[]byte("nokey")})
		assertEqualInt(t, reply, 0)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []interface{}{}})
		reply := execSCard(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSMembers(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("x"), []byte("y"), []byte("z")})

	t.Run("normal", func(t *testing.T) {
		reply := execSMembers(db, [][]byte{[]byte("s")})
		members := getMultiBulkValues(t, reply)
		// Convert to set for order-insensitive comparison
		memberMap := make(map[string]bool)
		for _, m := range members {
			memberMap[string(m)] = true
		}
		if !memberMap["x"] || !memberMap["y"] || !memberMap["z"] || len(memberMap) != 3 {
			t.Errorf("unexpected members: %v", members)
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execSMembers(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil) // Null Bulk
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("int", &types.DataEntity{Data: int64(42)})
		reply := execSMembers(db, [][]byte{[]byte("int")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSRandMember(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("a"), []byte("b"), []byte("c")})

	t.Run("default count=1", func(t *testing.T) {
		reply := execSRandMember(db, [][]byte{[]byte("s")})
		val := getBulkValue(t, reply)
		if val == nil {
			t.Error("expected a member, got null")
		}
		// Should be one of a, b, c
		valid := map[string]bool{"a": true, "b": true, "c": true}
		if !valid[string(val)] {
			t.Errorf("unexpected random member: %q", val)
		}
	})

	t.Run("positive count", func(t *testing.T) {
		reply := execSRandMember(db, [][]byte{[]byte("s"), []byte("2")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 2 {
			t.Errorf("expected 2 members, got %d", len(members))
		}
		// Each should be in {"a","b","c"}
		valid := map[string]bool{"a": true, "b": true, "c": true}
		for _, m := range members {
			if !valid[string(m)] {
				t.Errorf("invalid member: %q", m)
			}
		}
	})

	t.Run("negative count (allow duplicates)", func(t *testing.T) {
		reply := execSRandMember(db, [][]byte{[]byte("s"), []byte("-2")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 2 {
			t.Errorf("expected 2 members, got %d", len(members))
		}
	})

	t.Run("empty set", func(t *testing.T) {
		execSAdd(db, [][]byte{[]byte("empty"), []byte("temp")})
		execSRem(db, [][]byte{[]byte("empty"), []byte("temp")}) // now empty
		reply := execSRandMember(db, [][]byte{[]byte("empty")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execSRandMember(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("invalid count", func(t *testing.T) {
		reply := execSRandMember(db, [][]byte{[]byte("s"), []byte("abc")})
		errMsg := getErrorString(t, reply)
		if errMsg != "ERR value is not an integer or out of range" {
			t.Errorf("expected integer error, got: %s", errMsg)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execSRandMember(db, [][]byte{[]byte("str")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSPop(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s"), []byte("a"), []byte("b"), []byte("c")})

	t.Run("pop one", func(t *testing.T) {
		reply := execSPop(db, [][]byte{[]byte("s")})
		val := getBulkValue(t, reply)
		if val == nil {
			t.Error("expected a member")
		}
		valid := map[string]bool{"a": true, "b": true, "c": true}
		if !valid[string(val)] {
			t.Errorf("invalid pop value: %q", val)
		}

		// Verify size decreased
		assertEqualInt(t, execSCard(db, [][]byte{[]byte("s")}), 2)
	})

	t.Run("pop until empty (auto delete)", func(t *testing.T) {
		db2 := NewMockDB()
		execSAdd(db2, [][]byte{[]byte("temp"), []byte("only")})
		execSPop(db2, [][]byte{[]byte("temp")})
		if _, exists := db2.GetEntity("temp"); exists {
			t.Error("key should be deleted when set becomes empty")
		}
	})

	t.Run("key not exists", func(t *testing.T) {
		reply := execSPop(db, [][]byte{[]byte("nokey")})
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("list", &types.DataEntity{Data: []byte("list")})
		reply := execSPop(db, [][]byte{[]byte("list")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSUnion(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s1"), []byte("a"), []byte("b")})
	execSAdd(db, [][]byte{[]byte("s2"), []byte("b"), []byte("c")})
	execSAdd(db, [][]byte{[]byte("s3"), []byte("d")})

	t.Run("union of multiple sets", func(t *testing.T) {
		reply := execSUnion(db, [][]byte{[]byte("s1"), []byte("s2"), []byte("s3")})
		members := getMultiBulkValues(t, reply)
		expected := map[string]bool{"a": true, "b": true, "c": true, "d": true}
		if len(members) != 4 {
			t.Errorf("expected 4 members, got %d", len(members))
		}
		for _, m := range members {
			if !expected[string(m)] {
				t.Errorf("unexpected member: %q", m)
			}
		}
	})

	t.Run("one set", func(t *testing.T) {
		reply := execSUnion(db, [][]byte{[]byte("s1")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 2 {
			t.Errorf("expected 2 members")
		}
	})

	t.Run("non-existing key treated as empty", func(t *testing.T) {
		reply := execSUnion(db, [][]byte{[]byte("s1"), []byte("nokey")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 2 {
			t.Errorf("non-existing key should be ignored")
		}
	})

	t.Run("all keys non-existing", func(t *testing.T) {
		reply := execSUnion(db, [][]byte{[]byte("x"), []byte("y")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 0 {
			t.Errorf("expected empty set, got %d members", len(members))
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("str", &types.DataEntity{Data: "hello"})
		reply := execSUnion(db, [][]byte{[]byte("s1"), []byte("str")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}

func TestExecSInter(t *testing.T) {
	db := NewMockDB()
	execSAdd(db, [][]byte{[]byte("s1"), []byte("a"), []byte("b"), []byte("c")})
	execSAdd(db, [][]byte{[]byte("s2"), []byte("b"), []byte("c"), []byte("d")})
	execSAdd(db, [][]byte{[]byte("s3"), []byte("c"), []byte("e")})

	t.Run("intersection of three sets", func(t *testing.T) {
		reply := execSInter(db, [][]byte{[]byte("s1"), []byte("s2"), []byte("s3")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 1 || string(members[0]) != "c" {
			t.Errorf("expected ['c'], got %v", members)
		}
	})

	t.Run("no intersection", func(t *testing.T) {
		execSAdd(db, [][]byte{[]byte("s4"), []byte("x"), []byte("y")})
		reply := execSInter(db, [][]byte{[]byte("s1"), []byte("s4")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 0 {
			t.Errorf("expected empty intersection, got %v", members)
		}
	})

	t.Run("one set", func(t *testing.T) {
		reply := execSInter(db, [][]byte{[]byte("s1")})
		members := getMultiBulkValues(t, reply)
		if len(members) != 3 {
			t.Errorf("single set intersection should return itself")
		}
	})

	t.Run("non-existing key → empty result", func(t *testing.T) {
		reply := execSInter(db, [][]byte{[]byte("s1"), []byte("nokey")})
		if reply != resp.NullBulkReply {
			t.Errorf("intersection with non-existing key should be empty")
		}
	})

	t.Run("all keys non-existing", func(t *testing.T) {
		reply := execSInter(db, [][]byte{[]byte("x"), []byte("y")})
		// Returns Null Bulk when no sets exist
		assertEqualBulk(t, reply, nil)
	})

	t.Run("wrong type", func(t *testing.T) {
		db.PutEntity("hash", &types.DataEntity{Data: map[string][]byte{}})
		reply := execSInter(db, [][]byte{[]byte("s1"), []byte("hash")})
		errMsg := getErrorString(t, reply)
		if !strings.Contains(errMsg, "WRONGTYPE") {
			t.Errorf("expected WRONGTYPE, got: %s", errMsg)
		}
	})
}
