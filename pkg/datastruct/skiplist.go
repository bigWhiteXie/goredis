package datastruct

import (
	"math/rand"
	"strconv"
	"time"
)

const (
	MaxLevel    = 32
	Probability = 0.25
)

// SkipListElement 表示跳表中的元素
type SkipListElement struct {
	Member []byte
	Score  float64
}

// SkipListNode 节点结构
type SkipListNode struct {
	element  *SkipListElement
	forward  []*SkipListNode // 多级指针
	span     []int           // 每级跨度，span(i)表示当前节点跳到forward[i]跨越了多少0层节点
	backward *SkipListNode   // 双向链表
}

// SkipList 跳表结构
type SkipList struct {
	header *SkipListNode
	tail   *SkipListNode
	level  int
	length int
}

// NewSkipListNode 创建新的节点
func NewSkipListNode(level int, elem *SkipListElement) *SkipListNode {
	return &SkipListNode{
		element: elem,
		forward: make([]*SkipListNode, level),
		span:    make([]int, level),
	}
}

// NewSkipList 创建新的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		header: NewSkipListNode(MaxLevel, nil),
		level:  1,
	}
}

// randomLevel 生成随机节点高度
func randomLevel() int {
	level := 1
	rand.Seed(time.Now().UnixNano())
	for rand.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

func (s1 *SkipList) Head() *SkipListNode {
	return s1.header
}

// Insert 插入元素
func (sl *SkipList) Insert(score float64, member []byte) *SkipListNode {
	update := make([]*SkipListNode, MaxLevel)
	rank := make([]int, MaxLevel)

	x := sl.header
	// 从上往下开始遍历
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// 查找当前层级的元素
		for x.forward[i] != nil && (x.forward[i].element.Score < score ||
			(x.forward[i].element.Score == score && string(x.forward[i].element.Member) < string(member))) {
			rank[i] += x.span[i]
			x = x.forward[i]
		}
		update[i] = x
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			sl.header.span[i] = sl.length
		}
		sl.level = level
	}

	newNode := NewSkipListNode(level, &SkipListElement{Score: score, Member: append([]byte(nil), member...)})
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode

		newNode.span[i] = update[i].span[i] - (rank[0] - rank[i])
		update[i].span[i] = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].span[i]++
	}

	if update[0] == sl.header {
		newNode.backward = nil
	} else {
		newNode.backward = update[0]
	}

	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	} else {
		sl.tail = newNode
	}

	sl.length++
	return newNode
}

// Delete 删除指定元素
func (sl *SkipList) Delete(score float64, member []byte) bool {
	update := make([]*SkipListNode, MaxLevel)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && (x.forward[i].element.Score < score ||
			(x.forward[i].element.Score == score && string(x.forward[i].element.Member) < string(member))) {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]
	if x != nil && x.element.Score == score && string(x.element.Member) == string(member) {
		for i := 0; i < sl.level; i++ {
			if update[i].forward[i] == x {
				update[i].span[i] += x.span[i] - 1
				update[i].forward[i] = x.forward[i]
			} else {
				update[i].span[i]--
			}
		}

		if x.forward[0] != nil {
			x.forward[0].backward = x.backward
		} else {
			sl.tail = x.backward
		}

		for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
			sl.level--
		}

		sl.length--
		return true
	}
	return false
}

// GetRank 返回 member 的排名(0-based)
func (sl *SkipList) GetRank(score float64, member []byte) int {
	rank := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && (x.forward[i].element.Score < score ||
			(x.forward[i].element.Score == score && string(x.forward[i].element.Member) <= string(member))) {
			rank += x.span[i]
			x = x.forward[i]
		}
		if x != nil && string(x.element.Member) == string(member) {
			return rank - 1 // 0-based
		}
	}
	return -1
}

// GetByRank 按排名获取节点
func (sl *SkipList) GetByRank(rank int) *SkipListNode {
	if rank < 0 || rank >= sl.length {
		return nil
	}
	traversed := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && traversed+x.span[i] <= rank {
			traversed += x.span[i]
			x = x.forward[i]
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

// RangeToBytes 按正序返回 start-stop 的成员（支持 WITHSCORES）
func (sl *SkipList) RangeToBytes(start, stop int, withScores bool) [][]byte {
	if sl.length == 0 {
		return nil
	}

	// 处理负索引
	if start < 0 {
		start = sl.length + start
	}
	if stop < 0 {
		stop = sl.length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}
	if start > stop {
		return nil
	}

	var res [][]byte
	idx := 0
	for node := sl.header.forward[0]; node != nil; node = node.forward[0] {
		if idx >= start && idx <= stop {
			res = append(res, node.element.Member)
			if withScores {
				res = append(res, []byte(strconv.FormatFloat(node.element.Score, 'f', -1, 64)))
			}
		}
		idx++
		if idx > stop {
			break
		}
	}
	return res
}

// RangeByScore 返回跳表中 score 在 [min, max] 的所有节点，正序或逆序
func (sl *SkipList) RangeByScore(min, max float64, forward bool) []*SkipListNode {
	if sl.length == 0 {
		return nil
	}

	var nodes []*SkipListNode

	if forward {
		// 正序查找第一个 >= min 的节点
		x := sl.header
		for i := sl.level - 1; i >= 0; i-- {
			for x.forward[i] != nil && x.forward[i].element.Score < min {
				x = x.forward[i]
			}
		}
		x = x.forward[0]

		// 遍历到 max
		for x != nil && x.element.Score <= max {
			nodes = append(nodes, x)
			x = x.forward[0]
		}
	} else {
		// 逆序查找第一个 <= max 的节点
		x := sl.tail
		for x != nil && x.element.Score > max {
			x = x.backward
		}

		// 遍历到 min
		for x != nil && x.element.Score >= min {
			nodes = append(nodes, x)
			x = x.backward
		}
	}

	return nodes
}

// RangeNodes 返回 SkipList 中从 startRank 到 stopRank 的节点切片（0-based）
// 正序：forward=true，逆序：forward=false
func (sl *SkipList) RangeNodes(startRank, stopRank int, forward bool) []*SkipListNode {
	if startRank < 0 {
		startRank = 0
	}
	if stopRank >= sl.length {
		stopRank = sl.length - 1
	}
	if startRank > stopRank || sl.length == 0 {
		return nil
	}

	nodes := make([]*SkipListNode, 0, stopRank-startRank+1)

	if forward {
		// 正序遍历
		rank := 0
		x := sl.header
		for i := sl.level - 1; i >= 0; i-- {
			for x.forward[i] != nil && rank+x.span[i] <= startRank {
				rank += x.span[i]
				x = x.forward[i]
			}
		}
		// x.forward[0] 是起点
		x = x.forward[0]
		rank++
		for x != nil && rank-1 <= stopRank {
			nodes = append(nodes, x)
			x = x.forward[0]
			rank++
		}
	} else {
		// 逆序遍历
		// 找到 stopRank 对应节点
		var startNode *SkipListNode
		rank := 0
		x := sl.header
		for i := sl.level - 1; i >= 0; i-- {
			for x.forward[i] != nil && rank+x.span[i] <= stopRank {
				rank += x.span[i]
				x = x.forward[i]
			}
		}
		startNode = x.forward[0]
		if startNode == nil {
			return nil
		}
		// 从 stopRank 开始向前遍历
		rank = stopRank
		for x := startNode; x != nil && rank >= startRank; x = x.backward {
			nodes = append(nodes, x)
			rank--
		}
	}

	return nodes
}

func (sl *SkipList) FirstGreaterEqual(target float64) *SkipListNode {
	x := sl.header
	// 从最高层向下查找
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].element.Score < target {
			x = x.forward[i]
		}
	}
	// x.forward[0] 就是第一个 >= target 的节点（可能为 nil）
	return x.forward[0]
}

// Len 返回长度
func (sl *SkipList) Len() int {
	return sl.length
}

func (sl *SkipList) Level() int {
	return sl.level
}

func (sl *SkipList) ReverseRangeToBytes(start, stop int, withScores bool) [][]byte {
	if sl.length == 0 {
		return nil
	}

	if start < 0 {
		start = sl.length + start
	}
	if stop < 0 {
		stop = sl.length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}
	if start > stop {
		return nil
	}

	var res [][]byte
	idx := 0
	// 从 tail 开始遍历
	for node := sl.tail; node != nil; node = node.backward {
		if idx >= start && idx <= stop {
			res = append(res, node.element.Member)
			if withScores {
				res = append(res, []byte(strconv.FormatFloat(node.element.Score, 'f', -1, 64)))
			}
		}
		idx++
		if idx > stop {
			break
		}
	}
	return res
}
