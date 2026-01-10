package datastruct

import (
	"reflect"
	"testing"
)

func TestSkipList(t *testing.T) {
	t.Run("basic insert and delete", func(t *testing.T) {
		sl := NewSkipList()

		// Insert
		node1 := sl.Insert(1.0, []byte("a"))
		if sl.Len() != 1 || sl.Level() < 1 {
			t.Errorf("After insert: len=%d, level=%d", sl.Len(), sl.Level())
		}
		if string(node1.element.Member) != "a" || node1.element.Score != 1.0 {
			t.Errorf("Node content mismatch: %v", node1.element)
		}

		// Insert duplicate score with different member
		sl.Insert(1.0, []byte("b"))
		if sl.Len() != 2 {
			t.Errorf("Len should be 2, got %d", sl.Len())
		}

		// Insert higher score
		sl.Insert(2.0, []byte("c"))
		if sl.Len() != 3 {
			t.Errorf("Len should be 3, got %d", sl.Len())
		}

		// Delete
		ok := sl.Delete(1.0, []byte("a"))
		if !ok || sl.Len() != 2 {
			t.Errorf("Delete failed: ok=%v, len=%d", ok, sl.Len())
		}

		// Delete non-existing
		ok = sl.Delete(99.0, []byte("x"))
		if ok {
			t.Error("Delete non-existing should return false")
		}
	})

	t.Run("GetRank and GetByRank", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(3.0, []byte("c")) // [a(1.0), b(2.0), c(3.0)]

		// GetRank (0-based)
		rank := sl.GetRank(1.0, []byte("a"))
		if rank != 0 {
			t.Errorf("Rank of 'a' should be 0, got %d", rank)
		}

		rank = sl.GetRank(2.0, []byte("b"))
		if rank != 1 {
			t.Errorf("Rank of 'b' should be 1, got %d", rank)
		}

		rank = sl.GetRank(3.0, []byte("c"))
		if rank != 2 {
			t.Errorf("Rank of 'c' should be 2, got %d", rank)
		}

		// GetByRank
		node := sl.GetByRank(0)
		if node == nil || string(node.element.Member) != "a" {
			t.Errorf("GetByRank(0) failed: %v", node)
		}

		node = sl.GetByRank(2)
		if node == nil || string(node.element.Member) != "c" {
			t.Errorf("GetByRank(2) failed: %v", node)
		}

		// Out of range
		node = sl.GetByRank(3)
		if node != nil {
			t.Error("GetByRank(3) should return nil")
		}

		node = sl.GetByRank(-1)
		if node != nil {
			t.Error("GetByRank(-1) should return nil")
		}
	})

	t.Run("RangeToBytes", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(3.0, []byte("c"))

		// Normal range
		result := sl.RangeToBytes(0, 1, false)
		expected := [][]byte{[]byte("a"), []byte("b")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("RangeToBytes(0,1) failed: got %v", result)
		}

		// With scores
		result = sl.RangeToBytes(1, 2, true)
		expected = [][]byte{[]byte("b"), []byte("2"), []byte("c"), []byte("3")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("RangeToBytes with scores failed: got %v", result)
		}

		// Negative indices
		result = sl.RangeToBytes(-2, -1, false)
		expected = [][]byte{[]byte("b"), []byte("c")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("RangeToBytes(-2,-1) failed: got %v", result)
		}

		// Out of bounds
		result = sl.RangeToBytes(-10, 10, false)
		expected = [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("RangeToBytes out of bounds failed: got %v", result)
		}

		// Invalid range
		result = sl.RangeToBytes(2, 0, false)
		if result != nil {
			t.Errorf("Invalid range should return nil, got %v", result)
		}

		// Empty list
		empty := NewSkipList()
		result = empty.RangeToBytes(0, 1, false)
		if result != nil {
			t.Errorf("Empty list range should return nil, got %v", result)
		}
	})

	t.Run("ReverseRangeToBytes", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(3.0, []byte("c")) // forward: [a,b,c], reverse: [c,b,a]

		// Normal range
		result := sl.ReverseRangeToBytes(0, 1, false)
		expected := [][]byte{[]byte("c"), []byte("b")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("ReverseRangeToBytes(0,1) failed: got %v", result)
		}

		// With scores
		result = sl.ReverseRangeToBytes(1, 2, true)
		expected = [][]byte{[]byte("b"), []byte("2"), []byte("a"), []byte("1")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("ReverseRangeToBytes with scores failed: got %v", result)
		}

		// Negative indices
		result = sl.ReverseRangeToBytes(-2, -1, false)
		expected = [][]byte{[]byte("b"), []byte("a")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("ReverseRangeToBytes(-2,-1) failed: got %v", result)
		}
	})

	t.Run("RangeByScore", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(2.5, []byte("bb"))
		sl.Insert(3.0, []byte("c"))

		// Forward range
		nodes := sl.RangeByScore(2.0, 3.0, true)
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes in [2.0,3.0], got %d", len(nodes))
		}
		expectedMembers := []string{"b", "bb", "c"}
		for i, node := range nodes {
			if string(node.element.Member) != expectedMembers[i] {
				t.Errorf("Node %d: expected %s, got %s", i, expectedMembers[i], node.element.Member)
			}
		}

		// Reverse range
		nodes = sl.RangeByScore(2.0, 3.0, false)
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes in reverse [2.0,3.0], got %d", len(nodes))
		}
		expectedMembers = []string{"c", "bb", "b"}
		for i, node := range nodes {
			if string(node.element.Member) != expectedMembers[i] {
				t.Errorf("Reverse node %d: expected %s, got %s", i, expectedMembers[i], node.element.Member)
			}
		}

		// No matches
		nodes = sl.RangeByScore(10.0, 20.0, true)
		if len(nodes) != 0 {
			t.Errorf("Expected 0 nodes, got %d", len(nodes))
		}
	})

	t.Run("RangeNodes", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(3.0, []byte("c"))

		// Forward range
		nodes := sl.RangeNodes(0, 1, true)
		if len(nodes) != 2 {
			t.Errorf("Forward RangeNodes(0,1) failed, got %d", len(nodes))
		}
		if string(nodes[0].element.Member) != "a" || string(nodes[1].element.Member) != "b" {
			t.Errorf("Forward range content mismatch: %v", nodes)
		}

		// Reverse range
		nodes = sl.RangeNodes(0, 1, false)
		if len(nodes) != 2 {
			t.Errorf("Reverse RangeNodes(0,1) failed, got %d", len(nodes))
		}
		if string(nodes[0].element.Member) != "c" || string(nodes[1].element.Member) != "b" {
			t.Errorf("Reverse range content mismatch: %v", nodes)
		}

		// Boundary conditions
		nodes = sl.RangeNodes(-1, 10, true)
		if len(nodes) != 3 {
			t.Errorf("Boundary range failed, got %d", len(nodes))
		}
	})

	t.Run("FirstGreaterEqual", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("a"))
		sl.Insert(2.0, []byte("b"))
		sl.Insert(3.0, []byte("c"))

		// Exact match
		node := sl.FirstGreaterEqual(2.0)
		if node == nil || string(node.element.Member) != "b" {
			t.Errorf("FirstGreaterEqual(2.0) failed: %v", node)
		}

		// Between scores
		node = sl.FirstGreaterEqual(2.5)
		if node == nil || string(node.element.Member) != "c" {
			t.Errorf("FirstGreaterEqual(2.5) failed: %v", node)
		}

		// Greater than all
		node = sl.FirstGreaterEqual(4.0)
		if node != nil {
			t.Errorf("FirstGreaterEqual(4.0) should return nil, got %v", node)
		}

		// Less than all
		node = sl.FirstGreaterEqual(0.5)
		if node == nil || string(node.element.Member) != "a" {
			t.Errorf("FirstGreaterEqual(0.5) failed: %v", node)
		}
	})

	t.Run("binary safety", func(t *testing.T) {
		sl := NewSkipList()
		binaryMember1 := []byte{0, 1, 255}
		binaryMember2 := []byte{255, 1, 0}

		sl.Insert(1.0, binaryMember1)
		sl.Insert(2.0, binaryMember2)

		// Verify insertion
		node1 := sl.GetByRank(0)
		if !reflect.DeepEqual(node1.element.Member, binaryMember1) {
			t.Errorf("Binary member 1 mismatch: got %v", node1.element.Member)
		}

		node2 := sl.GetByRank(1)
		if !reflect.DeepEqual(node2.element.Member, binaryMember2) {
			t.Errorf("Binary member 2 mismatch: got %v", node2.element.Member)
		}

		// Delete by binary member
		ok := sl.Delete(1.0, binaryMember1)
		if !ok || sl.Len() != 1 {
			t.Errorf("Binary delete failed: ok=%v, len=%d", ok, sl.Len())
		}

		// Range with binary members
		result := sl.RangeToBytes(0, 0, false)
		if !reflect.DeepEqual(result[0], binaryMember2) {
			t.Errorf("Binary range failed: got %v", result[0])
		}
	})

	t.Run("duplicate scores", func(t *testing.T) {
		sl := NewSkipList()
		sl.Insert(1.0, []byte("z"))
		sl.Insert(1.0, []byte("a"))
		sl.Insert(1.0, []byte("m")) // Should be ordered: a, m, z

		// Verify order
		result := sl.RangeToBytes(0, 2, false)
		expected := [][]byte{[]byte("a"), []byte("m"), []byte("z")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Duplicate scores ordering failed: got %v", result)
		}

		// Ranks
		if sl.GetRank(1.0, []byte("a")) != 0 ||
			sl.GetRank(1.0, []byte("m")) != 1 ||
			sl.GetRank(1.0, []byte("z")) != 2 {
			t.Error("Ranks for duplicate scores incorrect")
		}
	})

	t.Run("empty skiplist", func(t *testing.T) {
		sl := NewSkipList()

		if sl.Len() != 0 {
			t.Errorf("Empty skiplist len should be 0, got %d", sl.Len())
		}

		if sl.GetRank(1.0, []byte("x")) != -1 {
			t.Error("GetRank on empty should return -1")
		}

		if sl.GetByRank(0) != nil {
			t.Error("GetByRank on empty should return nil")
		}

		if sl.FirstGreaterEqual(1.0) != nil {
			t.Error("FirstGreaterEqual on empty should return nil")
		}
	})
}
