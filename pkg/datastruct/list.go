package datastruct

type Node struct {
	prev  *Node
	next  *Node
	value interface{}
}

func (n *Node) Prev() *Node { return n.prev }
func (n *Node) Next() *Node { return n.next }
func (n *Node) Value() interface{} {
	return n.value
}

type List struct {
	head *Node
	tail *Node
	len  int
}

func NewList() *List {
	return &List{}
}

func (l *List) Len() int {
	return l.len
}

func (l *List) Head() *Node {
	return l.head
}

func (l *List) Tail() *Node {
	return l.tail
}

func (l *List) PushFront(value interface{}) *Node {
	node := &Node{value: value}

	if l.len == 0 {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}

	l.len++
	return node
}

func (l *List) PushBack(value interface{}) *Node {
	node := &Node{value: value}

	if l.len == 0 {
		l.head = node
		l.tail = node
	} else {
		node.prev = l.tail
		l.tail.next = node
		l.tail = node
	}

	l.len++
	return node
}

func (l *List) Remove(node *Node) {
	if node == nil || l.len == 0 {
		return
	}

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}

	node.prev = nil
	node.next = nil
	l.len--
}

func (l *List) PopFront() interface{} {
	if l.len == 0 {
		return nil
	}
	node := l.head
	l.Remove(node)
	return node.value
}

func (l *List) PopBack() interface{} {
	if l.len == 0 {
		return nil
	}
	node := l.tail
	l.Remove(node)
	return node.value
}

func (l *List) Get(index int) (interface{}, bool) {
	if index < 0 {
		index = l.len + index
	}
	if index < 0 || index >= l.len {
		return nil, false
	}

	var cur *Node
	if index < l.len/2 {
		cur = l.head
		for i := 0; i < index; i++ {
			cur = cur.next
		}
	} else {
		cur = l.tail
		for i := l.len - 1; i > index; i-- {
			cur = cur.prev
		}
	}

	return cur.value, true
}

func (l *List) Range(start, stop int) []interface{} {
	if l.len == 0 {
		return nil
	}

	if start < 0 {
		start = l.len + start
	}
	if stop < 0 {
		stop = l.len + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= l.len {
		stop = l.len - 1
	}
	if start > stop {
		return nil
	}

	result := make([]interface{}, 0, stop-start+1)

	cur, _ := l.GetNode(start)
	for i := start; i <= stop && cur != nil; i++ {
		result = append(result, cur.value)
		cur = cur.next
	}
	return result
}

func (l *List) GetNode(index int) (*Node, bool) {
	if index < 0 {
		index = l.len + index
	}
	if index < 0 || index >= l.len {
		return nil, false
	}

	cur := l.head
	for i := 0; i < index; i++ {
		cur = cur.next
	}
	return cur, true
}

func (l *List) Set(index int, value interface{}) bool {
	node, ok := l.GetNode(index)
	if !ok {
		return false
	}
	node.value = value
	return true
}

// count > 0: 从头删 count 个
// count < 0: 从尾删 |count| 个
// count = 0: 删除所有
func (l *List) RemoveByValue(count int, value interface{}) int {
	removed := 0

	if count == 0 {
		for cur := l.head; cur != nil; {
			next := cur.next
			if cur.value == value {
				l.Remove(cur)
				removed++
			}
			cur = next
		}
		return removed
	}

	if count > 0 {
		for cur := l.head; cur != nil && removed < count; {
			next := cur.next
			if cur.value == value {
				l.Remove(cur)
				removed++
			}
			cur = next
		}
		return removed
	}

	// count < 0
	count = -count
	for cur := l.tail; cur != nil && removed < count; {
		prev := cur.prev
		if cur.value == value {
			l.Remove(cur)
			removed++
		}
		cur = prev
	}
	return removed
}

func (l *List) InsertBefore(pivot *Node, value interface{}) *Node {
	if pivot == nil {
		return nil
	}
	node := &Node{value: value}
	node.prev = pivot.prev
	node.next = pivot

	if pivot.prev != nil {
		pivot.prev.next = node
	} else {
		l.head = node
	}
	pivot.prev = node

	l.len++
	return node
}

func (l *List) InsertAfter(pivot *Node, value interface{}) *Node {
	if pivot == nil {
		return nil
	}
	node := &Node{value: value}
	node.next = pivot.next
	node.prev = pivot

	if pivot.next != nil {
		pivot.next.prev = node
	} else {
		l.tail = node
	}
	pivot.next = node

	l.len++
	return node
}
