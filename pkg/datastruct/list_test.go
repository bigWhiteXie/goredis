package datastruct

import (
	"reflect"
	"testing"
)

func TestList(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		list := NewList()

		// Empty list
		if list.Len() != 0 {
			t.Errorf("Empty list len should be 0, got %d", list.Len())
		}
		if list.Head() != nil || list.Tail() != nil {
			t.Error("Empty list head/tail should be nil")
		}

		// PushFront
		node1 := list.PushFront("first")
		if list.Len() != 1 {
			t.Errorf("Len should be 1, got %d", list.Len())
		}
		if list.Head() != node1 || list.Tail() != node1 {
			t.Error("Head and tail should be the same node")
		}

		// PushBack
		node2 := list.PushBack("second")
		if list.Len() != 2 {
			t.Errorf("Len should be 2, got %d", list.Len())
		}
		if list.Head() != node1 || list.Tail() != node2 {
			t.Error("Head/tail mismatch after PushBack")
		}

		// PopFront
		val := list.PopFront()
		if val != "first" {
			t.Errorf("PopFront should return 'first', got %v", val)
		}
		if list.Len() != 1 {
			t.Errorf("Len should be 1 after PopFront, got %d", list.Len())
		}

		// PopBack
		val = list.PopBack()
		if val != "second" {
			t.Errorf("PopBack should return 'second', got %v", val)
		}
		if list.Len() != 0 {
			t.Errorf("List should be empty, len=%d", list.Len())
		}
	})

	t.Run("Get and Set", func(t *testing.T) {
		list := NewList()
		list.PushBack("a")
		list.PushBack("b")
		list.PushBack("c") // [a, b, c]

		// Positive index
		val, ok := list.Get(1)
		if !ok || val != "b" {
			t.Errorf("Get(1) failed: ok=%v, val=%v", ok, val)
		}

		// Negative index
		val, ok = list.Get(-1)
		if !ok || val != "c" {
			t.Errorf("Get(-1) failed: ok=%v, val=%v", ok, val)
		}

		val, ok = list.Get(-3)
		if !ok || val != "a" {
			t.Errorf("Get(-3) failed: ok=%v, val=%v", ok, val)
		}

		// Out of range
		_, ok = list.Get(3)
		if ok {
			t.Error("Get(3) should fail")
		}
		_, ok = list.Get(-4)
		if ok {
			t.Error("Get(-4) should fail")
		}

		// Set
		ok = list.Set(1, "B")
		if !ok {
			t.Error("Set(1) should succeed")
		}
		val, _ = list.Get(1)
		if val != "B" {
			t.Errorf("Set failed: expected 'B', got %v", val)
		}

		// Set out of range
		ok = list.Set(10, "X")
		if ok {
			t.Error("Set(10) should fail")
		}
	})

	t.Run("Range", func(t *testing.T) {
		list := NewList()
		for _, v := range []string{"a", "b", "c", "d", "e"} {
			list.PushBack(v)
		} // [a, b, c, d, e]

		// Normal range
		result := list.Range(1, 3)
		expected := []interface{}{"b", "c", "d"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(1,3) failed: got %v", result)
		}

		// Negative indices
		result = list.Range(-3, -1)
		expected = []interface{}{"c", "d", "e"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(-3,-1) failed: got %v", result)
		}

		// Out of bounds (clamped)
		result = list.Range(-10, 10)
		expected = []interface{}{"a", "b", "c", "d", "e"}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(-10,10) failed: got %v", result)
		}

		// Invalid range
		result = list.Range(3, 1)
		if result != nil {
			t.Errorf("Range(3,1) should return nil, got %v", result)
		}

		// Empty list
		empty := NewList()
		result = empty.Range(0, 1)
		if result != nil {
			t.Errorf("Range on empty list should return nil, got %v", result)
		}
	})

	t.Run("RemoveByValue", func(t *testing.T) {
		list := NewList()
		for _, v := range []string{"x", "y", "x", "z", "x"} {
			list.PushBack(v)
		} // [x, y, x, z, x]

		// Remove first 2 occurrences
		removed := list.RemoveByValue(2, "x")
		if removed != 2 {
			t.Errorf("Expected 2 removed, got %d", removed)
		}
		expected := []interface{}{"y", "z", "x"}
		result := list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After removing 2 'x': got %v", result)
		}

		// Remove all occurrences
		list = NewList()
		for _, v := range []string{"a", "b", "a", "c", "a"} {
			list.PushBack(v)
		}
		removed = list.RemoveByValue(0, "a")
		if removed != 3 {
			t.Errorf("Expected 3 removed, got %d", removed)
		}
		expected = []interface{}{"b", "c"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After removing all 'a': got %v", result)
		}

		// Remove last 1 occurrence
		list = NewList()
		for _, v := range []string{"p", "q", "p", "r", "p"} {
			list.PushBack(v)
		}
		removed = list.RemoveByValue(-1, "p")
		if removed != 1 {
			t.Errorf("Expected 1 removed, got %d", removed)
		}
		expected = []interface{}{"p", "q", "p", "r"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After removing last 'p': got %v", result)
		}

		// Remove non-existing value
		removed = list.RemoveByValue(1, "nonexistent")
		if removed != 0 {
			t.Errorf("Expected 0 removed, got %d", removed)
		}
	})

	t.Run("InsertBefore and InsertAfter", func(t *testing.T) {
		list := NewList()
		list.PushBack("A")
		nodeC := list.PushBack("C") // [A, C]

		// Insert before
		nodeB := list.InsertBefore(nodeC, "B")
		if nodeB == nil {
			t.Fatal("InsertBefore returned nil")
		}
		expected := []interface{}{"A", "B", "C"}
		result := list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After InsertBefore: got %v", result)
		}

		// Insert after
		list.InsertAfter(nodeC, "D")
		expected = []interface{}{"A", "B", "C", "D"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After InsertAfter: got %v", result)
		}

		// Insert before head
		list.InsertBefore(list.Head(), "0")
		expected = []interface{}{"0", "A", "B", "C", "D"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After InsertBefore head: got %v", result)
		}

		// Insert after tail
		list.InsertAfter(list.Tail(), "Z")
		expected = []interface{}{"0", "A", "B", "C", "D", "Z"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After InsertAfter tail: got %v", result)
		}

		// Insert with nil pivot
		nilNode := list.InsertBefore(nil, "X")
		if nilNode != nil {
			t.Error("InsertBefore with nil pivot should return nil")
		}
		nilNode = list.InsertAfter(nil, "X")
		if nilNode != nil {
			t.Error("InsertAfter with nil pivot should return nil")
		}
	})

	t.Run("Remove node", func(t *testing.T) {
		list := NewList()
		node1 := list.PushBack("1")
		node2 := list.PushBack("2")
		node3 := list.PushBack("3") // [1, 2, 3]

		// Remove middle node
		list.Remove(node2)
		expected := []interface{}{"1", "3"}
		result := list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After removing middle node: got %v", result)
		}

		// Remove head
		list.Remove(node1)
		expected = []interface{}{"3"}
		result = list.Range(0, list.Len()-1)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("After removing head: got %v", result)
		}

		// Remove tail
		list.Remove(node3)
		if list.Len() != 0 {
			t.Errorf("List should be empty, len=%d", list.Len())
		}

		// Remove from empty list
		list.Remove(node1) // should not panic
	})

	t.Run("binary safety", func(t *testing.T) {
		list := NewList()
		binaryVal := []byte{0, 1, 255}

		node := list.PushBack(binaryVal)
		val, _ := list.Get(0)
		if !reflect.DeepEqual(val, binaryVal) {
			t.Errorf("Binary value mismatch: got %v", val)
		}

		popped := list.PopFront()
		if !reflect.DeepEqual(popped, binaryVal) {
			t.Errorf("Popped binary value mismatch: got %v", popped)
		}

		// Test with node operations
		node = list.PushBack("test")
		list.InsertAfter(node, binaryVal)
		val, _ = list.Get(1)
		if !reflect.DeepEqual(val, binaryVal) {
			t.Errorf("Inserted binary value mismatch: got %v", val)
		}
	})
}
