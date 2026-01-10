package data

import (
	"reflect"
	"sort"
	"strconv"
	"testing"
)

// ---------- 基础功能 ----------

func TestNewSet(t *testing.T) {
	s := NewSet()
	if s.Len() != 0 || s.encoding != EncIntSet {
		t.Fatal("NewSet should start empty with EncIntSet")
	}
}

func TestAddAndContains(t *testing.T) {
	s := NewSet()
	// 整数
	if !s.Add([]byte("123")) || s.Add([]byte("123")) {
		t.Error("Add should return true then false for duplicate")
	}
	if !s.Contains([]byte("123")) || s.Contains([]byte("456")) {
		t.Error("Contains logic error")
	}
	// 非整数触发升级
	if !s.Add([]byte("abc")) {
		t.Error("Add non-int should succeed")
	}
	if s.encoding != EncHash {
		t.Error("encoding should be EncHash after upgrade")
	}
	// 升级后仍能加整数
	if !s.Add([]byte("789")) {
		t.Error("Add int after upgrade failed")
	}
}

func TestUpgradeThreshold(t *testing.T) {
	s := NewSet()
	// 写入 512 个不同整数，应保持 IntSet
	for i := 0; i < maxIntSetEntries; i++ {
		s.Add([]byte(strconv.Itoa(i)))
	}
	if s.encoding != EncIntSet || s.Len() != maxIntSetEntries {
		t.Fatalf("expected EncIntSet with %d items", maxIntSetEntries)
	}
	// 第 513 个触发升级
	s.Add([]byte("513"))
	if s.encoding != EncHash || s.Len() != maxIntSetEntries+1 {
		t.Fatalf("upgrade failed: encoding=%v, len=%d", s.encoding, s.Len())
	}
	// 原整数仍存在
	for i := 0; i < maxIntSetEntries; i++ {
		if !s.Contains([]byte(strconv.Itoa(i))) {
			t.Errorf("lost member %d after upgrade", i)
		}
	}
}

func TestRemove(t *testing.T) {
	s := NewSet()
	// IntSet 阶段删除
	s.Add([]byte("1"))
	if !s.Remove([]byte("1")) || s.Remove([]byte("1")) || s.Len() != 0 {
		t.Error("Remove int failed")
	}
	// Hash 阶段删除
	s.Add([]byte("a"))
	if !s.Remove([]byte("a")) || s.Len() != 0 {
		t.Error("Remove string failed")
	}
	// 删除不存在的元素
	if s.Remove([]byte("999")) {
		t.Error("Remove non-exist should return false")
	}
}

func TestMembers(t *testing.T) {
	s := NewSet()
	input := []string{"3", "1", "2"}
	for _, v := range input {
		s.Add([]byte(v))
	}
	mem := s.Members()
	strMem := make([]string, len(mem))
	for i, b := range mem {
		strMem[i] = string(b)
	}
	sort.Strings(strMem)
	if !reflect.DeepEqual(strMem, []string{"1", "2", "3"}) {
		t.Errorf("Members mismatch: %v", strMem)
	}
}

// ---------- 随机操作 ----------

func TestRandomAndPop(t *testing.T) {
	s := NewSet()
	// 空保护
	if _, ok := s.Random(); ok {
		t.Error("Random on empty should return false")
	}
	if _, ok := s.Pop(); ok {
		t.Error("Pop on empty should return false")
	}

	// 写入 100 个唯一值
	vals := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		s.Add([]byte(strconv.Itoa(i)))
		vals[strconv.Itoa(i)] = struct{}{}
	}

	// Random 1000 次，值必须合法
	for i := 0; i < 1000; i++ {
		b, ok := s.Random()
		if !ok {
			t.Fatal("Random failed")
		}
		if _, exist := vals[string(b)]; !exist {
			t.Errorf("Random returned illegal value %s", string(b))
		}
	}

	// Pop 全部
	popCount := 0
	for s.Len() > 0 {
		b, ok := s.Pop()
		if !ok {
			t.Fatal("Pop failed")
		}
		if _, exist := vals[string(b)]; !exist {
			t.Errorf("Pop returned illegal value %s", string(b))
		}
		popCount++
	}
	if popCount != 100 {
		t.Errorf("Pop count mismatch: %d", popCount)
	}
}

// ---------- 性能基准 ----------

func BenchmarkAddInt(b *testing.B) {
	s := NewSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add([]byte(strconv.Itoa(i % 10000)))
	}
}

func BenchmarkAddString(b *testing.B) {
	s := NewSet()
	// 强制升级一次
	s.Add([]byte("upgrade"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add([]byte(strconv.Itoa(i % 10000)))
	}
}

func BenchmarkContains(b *testing.B) {
	s := NewSet()
	for i := 0; i < 10000; i++ {
		s.Add([]byte(strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains([]byte(strconv.Itoa(i % 10000)))
	}
}
