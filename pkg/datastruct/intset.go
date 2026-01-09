package datastruct

import (
	"math/rand"
	"sort"
	"time"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// IntSet 是 Redis intset 的 Go 实现版本
// - 有序
// - 去重
// - 只存 int64
type IntSet struct {
	values []int64
}

// NewIntSet 创建一个空 IntSet
func NewIntSet() *IntSet {
	return &IntSet{
		values: make([]int64, 0),
	}
}

// Len 返回元素数量
func (is *IntSet) Len() int {
	return len(is.values)
}

// Values 返回一个拷贝（防止外部修改）
func (is *IntSet) Values() []int64 {
	cp := make([]int64, len(is.values))
	copy(cp, is.values)
	return cp
}

// Contains 判断是否存在
// O(log n)
func (is *IntSet) Contains(v int64) bool {
	idx := sort.Search(len(is.values), func(i int) bool {
		return is.values[i] >= v
	})
	return idx < len(is.values) && is.values[idx] == v
}

// Add 添加一个元素
// 返回 true 表示新增，false 表示已存在
// O(n)
func (is *IntSet) Add(v int64) bool {
	idx := sort.Search(len(is.values), func(i int) bool {
		return is.values[i] >= v
	})

	// 已存在
	if idx < len(is.values) && is.values[idx] == v {
		return false
	}

	// 插入
	is.values = append(is.values, 0)
	copy(is.values[idx+1:], is.values[idx:])
	is.values[idx] = v
	return true
}

// Remove 删除元素
// 返回 true 表示删除成功
// O(n)
func (is *IntSet) Remove(v int64) bool {
	idx := sort.Search(len(is.values), func(i int) bool {
		return is.values[i] >= v
	})

	if idx >= len(is.values) || is.values[idx] != v {
		return false
	}

	copy(is.values[idx:], is.values[idx+1:])
	is.values = is.values[:len(is.values)-1]
	return true
}

// Random 随机返回一个元素（不删除）
// 用于 SRANDMEMBER
func (is *IntSet) Random() (int64, bool) {
	if len(is.values) == 0 {
		return 0, false
	}
	// Go 的 map 没有顺序，但 slice 有
	// 用 rand.Intn 即可
	return is.values[randInt(len(is.values))], true
}

// Pop 随机弹出一个元素
func (is *IntSet) Pop() (int64, bool) {
	if len(is.values) == 0 {
		return 0, false
	}
	idx := randInt(len(is.values))
	val := is.values[idx]
	is.values = append(is.values[:idx], is.values[idx+1:]...)
	return val, true
}

func randInt(n int) int {
	return rng.Intn(n)
}
