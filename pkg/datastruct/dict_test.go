package datastruct

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentDict(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		dict := MakeConcurrent(16)

		// Put
		result := dict.Put("key1", "value1")
		if result != 1 {
			t.Errorf("Put should return 1 for new key, got %d", result)
		}
		if dict.Len() != 1 {
			t.Errorf("Len should be 1, got %d", dict.Len())
		}

		// Get
		val, exists := dict.Get("key1")
		if !exists || val != "value1" {
			t.Errorf("Get failed: exists=%v, val=%v", exists, val)
		}

		// Put (update)
		result = dict.Put("key1", "value2")
		if result != 0 {
			t.Errorf("Put should return 0 for update, got %d", result)
		}
		if dict.Len() != 1 {
			t.Errorf("Len should remain 1 after update, got %d", dict.Len())
		}

		// Remove
		result = dict.Remove("key1")
		if result != 1 {
			t.Errorf("Remove should return 1, got %d", result)
		}
		if dict.Len() != 0 {
			t.Errorf("Len should be 0 after remove, got %d", dict.Len())
		}

		// Get non-existing
		_, exists = dict.Get("key1")
		if exists {
			t.Error("Get should return false for non-existing key")
		}
	})

	t.Run("PutIfAbsent and PutIfExists", func(t *testing.T) {
		dict := MakeConcurrent(16)

		// PutIfAbsent on new key
		result := dict.PutIfAbsent("k1", "v1")
		if result != 1 || dict.Len() != 1 {
			t.Errorf("PutIfAbsent failed: result=%d, len=%d", result, dict.Len())
		}

		// PutIfAbsent on existing key
		result = dict.PutIfAbsent("k1", "v2")
		if result != 0 || dict.Len() != 1 {
			t.Errorf("PutIfAbsent should not update: result=%d, len=%d", result, dict.Len())
		}

		// PutIfExists on existing key
		result = dict.PutIfExists("k1", "v3")
		if result != 1 || dict.Len() != 1 {
			t.Errorf("PutIfExists failed: result=%d, len=%d", result, dict.Len())
		}

		// PutIfExists on non-existing key
		result = dict.PutIfExists("k2", "v4")
		if result != 0 || dict.Len() != 1 {
			t.Errorf("PutIfExists should not create: result=%d, len=%d", result, dict.Len())
		}
	})

	t.Run("Keys and ForEach", func(t *testing.T) {
		dict := MakeConcurrent(16)
		dict.Put("a", 1)
		dict.Put("b", 2)
		dict.Put("c", 3)

		keys := dict.Keys()
		if len(keys) != 3 {
			t.Errorf("Keys should return 3 keys, got %d", len(keys))
		}

		// Verify all keys exist
		keyMap := make(map[string]bool)
		for _, k := range keys {
			keyMap[k] = true
		}
		if !keyMap["a"] || !keyMap["b"] || !keyMap["c"] {
			t.Errorf("Missing keys in Keys(): %v", keys)
		}

		// ForEach
		var forEachKeys []string
		dict.ForEach(func(key string, val interface{}) bool {
			forEachKeys = append(forEachKeys, key)
			return true
		})
		if len(forEachKeys) != 3 {
			t.Errorf("ForEach should iterate 3 items, got %d", len(forEachKeys))
		}
	})

	t.Run("RandomKeys", func(t *testing.T) {
		dict := MakeConcurrent(16)
		for i := 0; i < 10; i++ {
			dict.Put(fmt.Sprintf("key%d", i), i)
		}

		// Request more than available
		random := dict.RandomKeys(15)
		if len(random) != 10 {
			t.Errorf("RandomKeys should return all keys when limit > size, got %d", len(random))
		}

		// Request less
		random = dict.RandomKeys(5)
		if len(random) != 5 {
			t.Errorf("RandomKeys should return 5 keys, got %d", len(random))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, k := range random {
			if seen[k] {
				t.Errorf("Duplicate key in RandomKeys: %s", k)
			}
			seen[k] = true
		}
	})

	t.Run("Clear", func(t *testing.T) {
		dict := MakeConcurrent(16)
		dict.Put("k1", "v1")
		dict.Put("k2", "v2")

		dict.Clear()
		if dict.Len() != 0 {
			t.Errorf("Clear should set len to 0, got %d", dict.Len())
		}

		_, exists := dict.Get("k1")
		if exists {
			t.Error("Key should not exist after Clear")
		}
	})

	t.Run("concurrent safety", func(t *testing.T) {
		dict := MakeConcurrent(16)
		const numWorkers = 10
		const numOps = 1000

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOps; j++ {
					key := fmt.Sprintf("worker%d-key%d", workerID, j%10)
					dict.Put(key, fmt.Sprintf("value-%d-%d", workerID, j))
					dict.Get(key)
					dict.Remove(key)
					dict.PutIfAbsent(key, "new")
					dict.PutIfExists(key, "update")
				}
			}(i)
		}

		wg.Wait()

		// Final state: each worker's last 10 keys should exist
		expectedCount := numWorkers * 10
		if dict.Len() != expectedCount {
			t.Errorf("Expected %d keys after concurrent ops, got %d", expectedCount, dict.Len())
		}
	})

	t.Run("empty dict", func(t *testing.T) {
		dict := MakeConcurrent(16)

		if dict.Len() != 0 {
			t.Errorf("Empty dict len should be 0, got %d", dict.Len())
		}

		keys := dict.Keys()
		if len(keys) != 0 {
			t.Errorf("Empty dict Keys should be empty, got %d", len(keys))
		}

		random := dict.RandomKeys(5)
		if len(random) != 0 {
			t.Errorf("Empty dict RandomKeys should be empty, got %d", len(random))
		}

		dict.ForEach(func(key string, val interface{}) bool {
			t.Error("ForEach should not be called on empty dict")
			return true
		})
	})
}
