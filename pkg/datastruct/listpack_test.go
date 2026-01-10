package datastruct

import (
	"reflect"
	"testing"
)

func TestListPack(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		lp := NewListPack(4)

		// Empty list
		if lp.Len() != 0 {
			t.Errorf("Empty list len should be 0, got %d", lp.Len())
		}

		// PushBack
		lp.PushBack([]byte("a"))
		lp.PushBack([]byte("b"))
		if lp.Len() != 2 {
			t.Errorf("Len should be 2, got %d", lp.Len())
		}

		// PushFront
		lp.PushFront([]byte("x"))
		expected := [][]byte{[]byte("x"), []byte("a"), []byte("b")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After PushFront: got %v", lp.data)
		}

		// PopFront
		val := lp.PopFront()
		if string(val) != "x" {
			t.Errorf("PopFront should return 'x', got %q", val)
		}
		if lp.Len() != 2 {
			t.Errorf("Len should be 2 after PopFront, got %d", lp.Len())
		}

		// PopBack
		val = lp.PopBack()
		if string(val) != "b" {
			t.Errorf("PopBack should return 'b', got %q", val)
		}
		if lp.Len() != 1 {
			t.Errorf("Len should be 1 after PopBack, got %d", lp.Len())
		}
	})

	t.Run("Get and Set", func(t *testing.T) {
		lp := NewListPack(3)
		lp.PushBack([]byte("alpha"))
		lp.PushBack([]byte("beta"))
		lp.PushBack([]byte("gamma")) // [alpha, beta, gamma]

		// Positive index
		val, ok := lp.Get(1)
		if !ok || string(val) != "beta" {
			t.Errorf("Get(1) failed: ok=%v, val=%q", ok, val)
		}

		// Negative index
		val, ok = lp.Get(-1)
		if !ok || string(val) != "gamma" {
			t.Errorf("Get(-1) failed: ok=%v, val=%q", ok, val)
		}

		val, ok = lp.Get(-3)
		if !ok || string(val) != "alpha" {
			t.Errorf("Get(-3) failed: ok=%v, val=%q", ok, val)
		}

		// Out of range
		_, ok = lp.Get(3)
		if ok {
			t.Error("Get(3) should fail")
		}
		_, ok = lp.Get(-4)
		if ok {
			t.Error("Get(-4) should fail")
		}

		// Set
		ok = lp.Set(1, []byte("BETA"))
		if !ok {
			t.Error("Set(1) should succeed")
		}
		val, _ = lp.Get(1)
		if string(val) != "BETA" {
			t.Errorf("Set failed: expected 'BETA', got %q", val)
		}

		// Set out of range
		ok = lp.Set(10, []byte("X"))
		if ok {
			t.Error("Set(10) should fail")
		}
	})

	t.Run("Range", func(t *testing.T) {
		lp := NewListPack(5)
		for _, v := range []string{"a", "b", "c", "d", "e"} {
			lp.PushBack([]byte(v))
		} // [a, b, c, d, e]

		// Normal range
		result := lp.Range(1, 3)
		expected := [][]byte{[]byte("b"), []byte("c"), []byte("d")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(1,3) failed: got %v", result)
		}

		// Negative indices
		result = lp.Range(-3, -1)
		expected = [][]byte{[]byte("c"), []byte("d"), []byte("e")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(-3,-1) failed: got %v", result)
		}

		// Out of bounds (clamped)
		result = lp.Range(-10, 10)
		expected = [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("Range(-10,10) failed: got %v", result)
		}

		// Invalid range
		result = lp.Range(3, 1)
		if result != nil {
			t.Errorf("Range(3,1) should return nil, got %v", result)
		}

		// Empty list
		empty := NewListPack(1)
		result = empty.Range(0, 1)
		if result != nil {
			t.Errorf("Range on empty list should return nil, got %v", result)
		}
	})

	t.Run("RemoveByValue", func(t *testing.T) {
		lp := NewListPack(10)
		for _, v := range []string{"x", "y", "x", "z", "x"} {
			lp.PushBack([]byte(v))
		} // [x, y, x, z, x]

		// Remove first 2 occurrences
		removed := lp.RemoveByValue(2, []byte("x"))
		if removed != 2 {
			t.Errorf("Expected 2 removed, got %d", removed)
		}
		expected := [][]byte{[]byte("y"), []byte("z"), []byte("x")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After removing 2 'x': got %v", lp.data)
		}

		// Remove all occurrences
		lp = NewListPack(10)
		for _, v := range []string{"a", "b", "a", "c", "a"} {
			lp.PushBack([]byte(v))
		}
		removed = lp.RemoveByValue(0, []byte("a"))
		if removed != 3 {
			t.Errorf("Expected 3 removed, got %d", removed)
		}
		expected = [][]byte{[]byte("b"), []byte("c")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After removing all 'a': got %v", lp.data)
		}

		// Remove last 1 occurrence
		lp = NewListPack(10)
		for _, v := range []string{"p", "q", "p", "r", "p"} {
			lp.PushBack([]byte(v))
		}
		removed = lp.RemoveByValue(-1, []byte("p"))
		if removed != 1 {
			t.Errorf("Expected 1 removed, got %d", removed)
		}
		expected = [][]byte{[]byte("p"), []byte("q"), []byte("p"), []byte("r")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After removing last 'p': got %v", lp.data)
		}

		// Remove non-existing value
		removed = lp.RemoveByValue(1, []byte("nonexistent"))
		if removed != 0 {
			t.Errorf("Expected 0 removed, got %d", removed)
		}

		// Binary safety in comparison
		binaryVal := []byte{0, 1, 255}
		lp = NewListPack(3)
		lp.PushBack(binaryVal)
		lp.PushBack([]byte("other"))
		lp.PushBack(binaryVal)

		removed = lp.RemoveByValue(1, binaryVal)
		if removed != 1 {
			t.Errorf("Binary value removal failed, got %d", removed)
		}
		if lp.Len() != 2 {
			t.Errorf("Len should be 2 after binary removal, got %d", lp.Len())
		}
	})

	t.Run("RemoveAt and Clear", func(t *testing.T) {
		lp := NewListPack(3)
		lp.PushBack([]byte("a"))
		lp.PushBack([]byte("b"))
		lp.PushBack([]byte("c")) // [a, b, c]

		// Remove middle
		lp.RemoveAt(1)
		expected := [][]byte{[]byte("a"), []byte("c")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After RemoveAt(1): got %v", lp.data)
		}

		// Remove head
		lp.RemoveAt(0)
		expected = [][]byte{[]byte("c")}
		if !reflect.DeepEqual(lp.data, expected) {
			t.Errorf("After RemoveAt(0): got %v", lp.data)
		}

		// Remove tail
		lp.RemoveAt(0)
		if lp.Len() != 0 {
			t.Errorf("List should be empty, len=%d", lp.Len())
		}

		// Remove out of range
		lp.RemoveAt(10) // should not panic
		lp.RemoveAt(-1) // should not panic

		// Clear
		lp.PushBack([]byte("temp"))
		lp.Clear()
		if lp.Len() != 0 {
			t.Errorf("Clear should empty list, len=%d", lp.Len())
		}
	})

	t.Run("binary safety", func(t *testing.T) {
		lp := NewListPack(2)
		binaryVal1 := []byte{0, 1, 255}
		binaryVal2 := []byte{255, 1, 0}

		lp.PushBack(binaryVal1)
		lp.PushBack(binaryVal2)

		// Get
		val, _ := lp.Get(0)
		if !reflect.DeepEqual(val, binaryVal1) {
			t.Errorf("Binary get failed: got %v", val)
		}

		// Range
		result := lp.Range(0, 1)
		if !reflect.DeepEqual(result[0], binaryVal1) || !reflect.DeepEqual(result[1], binaryVal2) {
			t.Errorf("Binary range failed: got %v", result)
		}

		// Pop
		popped := lp.PopFront()
		if !reflect.DeepEqual(popped, binaryVal1) {
			t.Errorf("Binary pop failed: got %v", popped)
		}

		// Set
		newBinary := []byte{128, 64}
		lp.Set(0, newBinary)
		val, _ = lp.Get(0)
		if !reflect.DeepEqual(val, newBinary) {
			t.Errorf("Binary set failed: got %v", val)
		}
	})

	t.Run("initial capacity", func(t *testing.T) {
		lp := NewListPack(100)
		if cap(lp.data) < 100 {
			t.Errorf("Initial capacity should be at least 100, got %d", cap(lp.data))
		}
	})
}
