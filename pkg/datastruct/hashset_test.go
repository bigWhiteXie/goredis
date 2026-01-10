package datastruct

import (
	"reflect"
	"testing"
)

func TestHashSet(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		set := NewHashSet()

		// Add new member
		result := set.Add([]byte("apple"))
		if !result {
			t.Error("Add should return true for new member")
		}
		if set.Len() != 1 {
			t.Errorf("Len should be 1, got %d", set.Len())
		}

		// Add duplicate
		result = set.Add([]byte("apple"))
		if result {
			t.Error("Add should return false for duplicate")
		}
		if set.Len() != 1 {
			t.Errorf("Len should remain 1, got %d", set.Len())
		}

		// Contains
		if !set.Contains([]byte("apple")) {
			t.Error("Contains should return true")
		}
		if set.Contains([]byte("banana")) {
			t.Error("Contains should return false for non-member")
		}

		// Remove
		result = set.Remove([]byte("apple"))
		if !result {
			t.Error("Remove should return true")
		}
		if set.Len() != 0 {
			t.Errorf("Len should be 0 after remove, got %d", set.Len())
		}

		// Remove non-existing
		result = set.Remove([]byte("apple"))
		if result {
			t.Error("Remove should return false for non-existing")
		}
	})

	t.Run("Members", func(t *testing.T) {
		set := NewHashSet()
		set.Add([]byte("x"))
		set.Add([]byte("y"))
		set.Add([]byte("z"))

		members := set.Members()
		if len(members) != 3 {
			t.Errorf("Members should return 3 items, got %d", len(members))
		}

		// Convert to string set for comparison
		memberMap := make(map[string]bool)
		for _, m := range members {
			memberMap[string(m)] = true
		}
		if !memberMap["x"] || !memberMap["y"] || !memberMap["z"] {
			t.Errorf("Missing members: %v", members)
		}
	})

	t.Run("Random", func(t *testing.T) {
		set := NewHashSet()

		// Empty set
		val, ok := set.Random()
		if ok || val != nil {
			t.Error("Random should return (nil, false) for empty set")
		}

		// Single member
		set.Add([]byte("single"))
		val, ok = set.Random()
		if !ok || string(val) != "single" {
			t.Errorf("Random failed: ok=%v, val=%q", ok, val)
		}

		// Multiple members
		set.Add([]byte("multi1"))
		set.Add([]byte("multi2"))
		val, ok = set.Random()
		if !ok {
			t.Error("Random should return true for non-empty set")
		}
		// Value should be one of the members
		valid := map[string]bool{"single": true, "multi1": true, "multi2": true}
		if !valid[string(val)] {
			t.Errorf("Random returned invalid value: %q", val)
		}
	})

	t.Run("Pop", func(t *testing.T) {
		set := NewHashSet()

		// Empty set
		val, ok := set.Pop()
		if ok || val != nil {
			t.Error("Pop should return (nil, false) for empty set")
		}

		// Single member
		set.Add([]byte("only"))
		val, ok = set.Pop()
		if !ok || string(val) != "only" {
			t.Errorf("Pop failed: ok=%v, val=%q", ok, val)
		}
		if set.Len() != 0 {
			t.Errorf("Set should be empty after pop, len=%d", set.Len())
		}

		// Multiple members
		set.Add([]byte("a"))
		set.Add([]byte("b"))
		set.Add([]byte("c"))
		initialLen := set.Len()

		val, ok = set.Pop()
		if !ok {
			t.Error("Pop should succeed on non-empty set")
		}
		if set.Len() != initialLen-1 {
			t.Errorf("Len should decrease by 1, expected %d, got %d", initialLen-1, set.Len())
		}
		// Verify popped value is no longer in set
		if set.Contains(val) {
			t.Errorf("Popped value %q should not be in set", val)
		}
	})

	t.Run("binary safety", func(t *testing.T) {
		set := NewHashSet()

		// Test with binary data (not valid UTF-8)
		binaryKey := []byte{0, 1, 2, 255}
		set.Add(binaryKey)

		if !set.Contains(binaryKey) {
			t.Error("Should contain binary key")
		}

		members := set.Members()
		if len(members) != 1 || !reflect.DeepEqual(members[0], binaryKey) {
			t.Errorf("Members should return original binary data: %v", members[0])
		}

		popped, ok := set.Pop()
		if !ok || !reflect.DeepEqual(popped, binaryKey) {
			t.Errorf("Pop should return original binary data: %v", popped)
		}
	})

	t.Run("empty set", func(t *testing.T) {
		set := NewHashSet()

		if set.Len() != 0 {
			t.Errorf("Empty set len should be 0, got %d", set.Len())
		}

		members := set.Members()
		if len(members) != 0 {
			t.Errorf("Empty set Members should be empty, got %d", len(members))
		}

		if set.Contains([]byte("anything")) {
			t.Error("Empty set should not contain anything")
		}
	})
}
