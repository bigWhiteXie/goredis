package datastruct

import (
	"sort"
	"testing"
)

// ---------- 基础功能 ----------

func TestNewAndLen(t *testing.T) {
	is := NewIntSet()
	if is.Len() != 0 {
		t.Errorf("new IntSet length should be 0, got %d", is.Len())
	}
	if v := is.Values(); len(v) != 0 {
		t.Errorf("Values() of empty set should be [], got %v", v)
	}
}

func TestAddAndContains(t *testing.T) {
	is := NewIntSet()
	// 重复写入
	if !is.Add(1) || is.Add(1) {
		t.Error("Add(1) should return true then false")
	}
	if !is.Contains(1) || is.Contains(2) {
		t.Error("Contains logic error")
	}
	// 批量写入 & 有序性校验
	for _, v := range []int64{5, 3, 9, 3, 5} {
		is.Add(v)
	}
	vals := is.Values()
	if !sort.SliceIsSorted(vals, func(i, j int) bool { return vals[i] < vals[j] }) {
		t.Error("values should be kept sorted")
	}
	if is.Len() != 4 {
		t.Errorf("expected len 4, got %d", is.Len())
	}
}

func TestRemove(t *testing.T) {
	is := NewIntSet()
	for i := int64(0); i < 10; i++ {
		is.Add(i)
	}
	// 删除头、中、尾、不存在
	for _, v := range []int64{0, 5, 9, 99} {
		before := is.Len()
		ok := is.Remove(v)
		if v == 99 {
			if ok || is.Len() != before {
				t.Error("remove non-exist should fail")
			}
			continue
		}
		if !ok || is.Len() != before-1 || is.Contains(v) {
			t.Errorf("remove %d failed", v)
		}
	}
}

// ---------- 随机操作 ----------

func TestRandomAndPop(t *testing.T) {
	is := NewIntSet()
	// 空集合
	if _, ok := is.Random(); ok {
		t.Error("Random() on empty set should return false")
	}
	if _, ok := is.Pop(); ok {
		t.Error("Pop() on empty set should return false")
	}

	// 写入 100 个唯一值
	set := make(map[int64]struct{})
	for i := 0; i < 100; i++ {
		for {
			v := rng.Int63n(1000)
			if is.Add(v) {
				set[v] = struct{}{}
				break
			}
		}
	}

	// Random 1000 次，值必须都在 set 中
	for i := 0; i < 1000; i++ {
		v, ok := is.Random()
		if !ok {
			t.Fatal("Random() should always succeed with elements")
		}
		if _, exist := set[v]; !exist {
			t.Errorf("Random() returned unexpected value %d", v)
		}
	}

	// Pop 全部元素
	remain := make(map[int64]struct{})
	for is.Len() > 0 {
		v, ok := is.Pop()
		if !ok {
			t.Fatal("Pop() should succeed")
		}
		if _, exist := set[v]; !exist {
			t.Errorf("Pop() returned unexpected value %d", v)
		}
		if _, dup := remain[v]; dup {
			t.Errorf("Pop() duplicated value %d", v)
		}
		remain[v] = struct{}{}
	}
	if len(remain) != len(set) {
		t.Error("Pop() count mismatch")
	}
}

// ---------- 性能 / 压力 ----------
func BenchmarkAdd(b *testing.B) {
	is := NewIntSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		is.Add(int64(i))
	}
}

func BenchmarkContains(b *testing.B) {
	is := NewIntSet()
	for i := 0; i < 10000; i++ {
		is.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		is.Contains(int64(i % 10000))
	}
}

func BenchmarkRemove(b *testing.B) {
	is := NewIntSet()
	vals := make([]int64, 10000)
	for i := range vals {
		vals[i] = int64(i)
		is.Add(vals[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		is.Remove(vals[i%10000])
	}
}
