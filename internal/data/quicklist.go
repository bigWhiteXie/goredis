package data

import (
	"goredis/internal/common"
	"goredis/internal/types"
	"goredis/pkg/datastruct"
)

const (
	lpMaxSize = 512
)

type List interface {
	types.RedisData

	// LPUSH
	PushFront(val []byte)

	// RPUSH
	PushBack(val []byte)

	// LPOP
	PopFront() []byte

	// RPOP
	PopBack() []byte

	// LLEN
	Len() int

	// LINDEX
	Get(index int) ([]byte, bool)

	// LSET
	Set(index int, val []byte) bool

	// LRANGE
	Range(start, stop int) [][]byte

	// LREM
	// 返回删除的元素个数
	RemoveByValue(count int, val []byte) int

	// LTRIM
	Trim(start, stop int)
}

var _ List = &QuickList{}

type QuickListNode struct {
	lp *datastruct.ListPack
}

type QuickList struct {
	list      *datastruct.List // 你已有的双向链表
	len       int              // 总元素数
	lpMaxSize int              // 单个 listpack 最大元素数
}

func NewQuickList() *QuickList {
	return &QuickList{
		list:      datastruct.NewList(),
		lpMaxSize: lpMaxSize,
	}
}

func newNode(size int) *QuickListNode {
	return &QuickListNode{
		lp: datastruct.NewListPack(size),
	}
}

func (ql *QuickList) headNode() *QuickListNode {
	if ql.list.Len() == 0 {
		return nil
	}
	return ql.list.Head().Value().(*QuickListNode)
}

func (ql *QuickList) tailNode() *QuickListNode {
	if ql.list.Len() == 0 {
		return nil
	}
	return ql.list.Tail().Value().(*QuickListNode)
}

// 拆成两个大小近似的节点， 避免频繁节点分裂

func (ql *QuickList) splitNode(node *datastruct.Node) {
	qn := node.Value().(*QuickListNode)
	lp := qn.lp
	if lp.Len() <= ql.lpMaxSize {
		return
	}

	mid := lp.Len() / 2
	size := lp.Len()
	newLP := datastruct.NewListPack(ql.lpMaxSize)

	// 新节点放置后一半的数据
	for i := mid; i < lp.Len(); i++ {
		newLP.PushBack(lp.Range(i, i)[0])
	}
	// 旧节点的后半段数据清理掉
	for i := 0; i < size-mid; i++ {
		lp.PopBack()
	}

	newNode := &QuickListNode{lp: newLP}
	ql.list.InsertAfter(node, newNode)
}

func (ql *QuickList) PushFront(val []byte) {
	head := ql.headNode()
	if head == nil {
		node := newNode(ql.lpMaxSize)
		node.lp.PushFront(val)
		ql.list.PushFront(node)
	} else {
		head.lp.PushFront(val)
		if head.lp.Len() > ql.lpMaxSize {
			ql.splitNode(ql.list.Head())
		}
	}
	ql.len++
}

func (ql *QuickList) PushBack(val []byte) {
	tail := ql.tailNode()
	if tail == nil {
		node := newNode(ql.lpMaxSize)
		node.lp.PushBack(val)
		ql.list.PushBack(node)
	} else {
		tail.lp.PushBack(val)
		if tail.lp.Len() > ql.lpMaxSize {
			ql.splitNode(ql.list.Tail())
		}
	}
	ql.len++
}

func (ql *QuickList) PopFront() []byte {
	headNode := ql.list.Head()
	if headNode == nil {
		return nil
	}

	qn := headNode.Value().(*QuickListNode)
	val := qn.lp.PopFront()
	if val == nil {
		return nil
	}

	if qn.lp.Len() == 0 {
		ql.list.Remove(headNode)
	}
	ql.len--
	return val
}

func (ql *QuickList) PopBack() []byte {
	tailNode := ql.list.Tail()
	if tailNode == nil {
		return nil
	}

	qn := tailNode.Value().(*QuickListNode)
	val := qn.lp.PopBack()
	if val == nil {
		return nil
	}

	if qn.lp.Len() == 0 {
		ql.list.Remove(tailNode)
	}
	ql.len--
	return val
}

func (ql *QuickList) Get(index int) ([]byte, bool) {
	if index < 0 {
		index = ql.len + index
	}
	if index < 0 || index >= ql.len {
		return nil, false
	}

	n := index
	for node := ql.list.Head(); node != nil; node = node.Next() {
		qn := node.Value().(*QuickListNode)
		if n < qn.lp.Len() {
			return qn.lp.Get(n)
		}
		n -= qn.lp.Len()
	}
	return nil, false
}

func (ql *QuickList) Range(start, stop int) [][]byte {
	if ql.len == 0 {
		return nil
	}

	if start < 0 {
		start = ql.len + start
	}
	if stop < 0 {
		stop = ql.len + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= ql.len {
		stop = ql.len - 1
	}
	if start > stop {
		return nil
	}

	var res [][]byte
	idx := 0

	for node := ql.list.Head(); node != nil; node = node.Next() {
		qn := node.Value().(*QuickListNode)
		for i := 0; i < qn.lp.Len(); i++ {
			if idx >= start && idx <= stop {
				v, _ := qn.lp.Get(i)
				res = append(res, v)
			}
			idx++
			if idx > stop {
				return res
			}
		}
	}
	return res
}

func (ql *QuickList) Set(index int, val []byte) bool {
	if index < 0 {
		index = ql.len + index
	}
	if index < 0 || index >= ql.len {
		return false
	}

	n := index
	for node := ql.list.Head(); node != nil; node = node.Next() {
		qn := node.Value().(*QuickListNode)
		if n < qn.lp.Len() {
			return qn.lp.Set(n, val)
		}
		n -= qn.lp.Len()
	}
	return false
}

func (ql *QuickList) RemoveByValue(count int, val []byte) int {
	removed := 0

	for node := ql.list.Head(); node != nil && (count == 0 || removed < common.Abs(count)); {
		next := node.Next()
		qn := node.Value().(*QuickListNode)

		n := qn.lp.RemoveByValue(count, val)
		removed += n

		if qn.lp.Len() == 0 {
			ql.list.Remove(node)
		}

		node = next
	}
	ql.len -= removed
	return removed
}

func (ql *QuickList) Trim(start, stop int) {
	if ql.len == 0 {
		return
	}

	start, stop, ok := normalizeRange(start, stop, ql.len)
	if !ok {
		// trim 后为空
		ql.list = datastruct.NewList()
		ql.len = 0
		return
	}

	// 需要删除的左边元素数量
	leftRemove := start
	// 需要删除的右边元素数量
	rightRemove := ql.len - stop - 1

	// 从左侧删除
	for i := 0; i < leftRemove; i++ {
		ql.PopFront()
	}

	// 从右侧删除
	for i := 0; i < rightRemove; i++ {
		ql.PopBack()
	}
}

func (ql *QuickList) Len() int {
	return ql.len
}

func (ql *QuickList) ToWriteCmdLine(key string) [][]byte {
	cmdLine := [][]byte{[]byte("rpush"), []byte(key)}
	results := ql.Range(0, ql.Len())
	for _, res := range results {
		cmdLine = append(cmdLine, res)
	}

	return cmdLine
}

func (ql *QuickList) Clone() interface{} {
	nl := NewQuickList()
	for _, member := range ql.Range(0, ql.len-1) {
		nl.PushBack(common.CloneBytes(member))
	}
	return nl
}

func normalizeRange(start, stop, length int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	if start > stop || start >= length || stop < 0 {
		return 0, 0, false
	}

	return start, stop, true
}
