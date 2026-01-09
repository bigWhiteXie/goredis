package data

import (
	"bytes"
	"testing"
)

func TestNewQuickList(t *testing.T) {
	ql := NewQuickList()
	if ql == nil {
		t.Fatal("Expected QuickList to be created, got nil")
	}
	if ql.len != 0 {
		t.Errorf("Expected len to be 0, got %d", ql.len)
	}
	if ql.list.Len() != 0 {
		t.Errorf("Expected underlying list to be empty, got %d", ql.list.Len())
	}
}

func TestQuickListPushFront(t *testing.T) {
	ql := NewQuickList()
	
	// 测试空列表添加第一个元素
	val1 := []byte("hello")
	ql.PushFront(val1)
	
	if ql.len != 1 {
		t.Errorf("Expected len to be 1, got %d", ql.len)
	}
	
	result, ok := ql.Get(0)
	if !ok || !bytes.Equal(result, val1) {
		t.Errorf("Expected to get %s, got %s", val1, result)
	}
	
	// 添加第二个元素
	val2 := []byte("world")
	ql.PushFront(val2)
	
	if ql.len != 2 {
		t.Errorf("Expected len to be 2, got %d", ql.len)
	}
	
	result, ok = ql.Get(0)
	if !ok || !bytes.Equal(result, val2) {
		t.Errorf("Expected to get %s, got %s", val2, result)
	}
	
	result, ok = ql.Get(1)
	if !ok || !bytes.Equal(result, val1) {
		t.Errorf("Expected to get %s, got %s", val1, result)
	}
}

func TestQuickListPushBack(t *testing.T) {
	ql := NewQuickList()
	
	// 测试空列表添加第一个元素
	val1 := []byte("first")
	ql.PushBack(val1)
	
	if ql.len != 1 {
		t.Errorf("Expected len to be 1, got %d", ql.len)
	}
	
	result, ok := ql.Get(0)
	if !ok || !bytes.Equal(result, val1) {
		t.Errorf("Expected to get %s, got %s", val1, result)
	}
	
	// 添加第二个元素
	val2 := []byte("second")
	ql.PushBack(val2)
	
	if ql.len != 2 {
		t.Errorf("Expected len to be 2, got %d", ql.len)
	}
	
	result, ok = ql.Get(0)
	if !ok || !bytes.Equal(result, val1) {
		t.Errorf("Expected to get %s, got %s", val1, result)
	}
	
	result, ok = ql.Get(1)
	if !ok || !bytes.Equal(result, val2) {
		t.Errorf("Expected to get %s, got %s", val2, result)
	}
}

func TestQuickListPopFront(t *testing.T) {
	ql := NewQuickList()
	
	// 测试从空列表弹出
	result := ql.PopFront()
	if result != nil {
		t.Errorf("Expected nil when popping from empty list, got %s", result)
	}
	
	// 添加一些值
	ql.PushBack([]byte("first"))
	ql.PushBack([]byte("second"))
	ql.PushBack([]byte("third"))
	
	// 弹出第一个值
	result = ql.PopFront()
	if !bytes.Equal(result, []byte("first")) {
		t.Errorf("Expected to pop 'first', got %s", result)
	}
	
	if ql.len != 2 {
		t.Errorf("Expected len to be 2, got %d", ql.len)
	}
	
	// 再次验证剩余元素
	result, ok := ql.Get(0)
	if !ok || !bytes.Equal(result, []byte("second")) {
		t.Errorf("Expected to get 'second', got %s", result)
	}
}

func TestQuickListPopBack(t *testing.T) {
	ql := NewQuickList()
	
	// 测试从空列表弹出
	result := ql.PopBack()
	if result != nil {
		t.Errorf("Expected nil when popping from empty list, got %s", result)
	}
	
	// 添加一些值
	ql.PushBack([]byte("first"))
	ql.PushBack([]byte("second"))
	ql.PushBack([]byte("third"))
	
	// 弹出最后一个值
	result = ql.PopBack()
	if !bytes.Equal(result, []byte("third")) {
		t.Errorf("Expected to pop 'third', got %s", result)
	}
	
	if ql.len != 2 {
		t.Errorf("Expected len to be 2, got %d", ql.len)
	}
	
	// 再次验证剩余元素
	result, ok := ql.Get(1)
	if !ok || !bytes.Equal(result, []byte("second")) {
		t.Errorf("Expected to get 'second', got %s", result)
	}
}

func TestQuickListGet(t *testing.T) {
	ql := NewQuickList()
	
	// 测试获取不存在的索引
	result, ok := ql.Get(0)
	if ok || result != nil {
		t.Errorf("Expected (nil, false) for empty list, got (%v, %v)", result, ok)
	}
	
	// 添加一些值
	ql.PushBack([]byte("first"))
	ql.PushBack([]byte("second"))
	ql.PushBack([]byte("third"))
	
	// 测试正向索引
	result, ok = ql.Get(0)
	if !ok || !bytes.Equal(result, []byte("first")) {
		t.Errorf("Expected to get 'first', got %s", result)
	}
	
	result, ok = ql.Get(1)
	if !ok || !bytes.Equal(result, []byte("second")) {
		t.Errorf("Expected to get 'second', got %s", result)
	}
	
	result, ok = ql.Get(2)
	if !ok || !bytes.Equal(result, []byte("third")) {
		t.Errorf("Expected to get 'third', got %s", result)
	}
	
	// 测试负向索引
	result, ok = ql.Get(-1)
	if !ok || !bytes.Equal(result, []byte("third")) {
		t.Errorf("Expected to get 'third' with negative index, got %s", result)
	}
	
	result, ok = ql.Get(-2)
	if !ok || !bytes.Equal(result, []byte("second")) {
		t.Errorf("Expected to get 'second' with negative index, got %s", result)
	}
	
	// 测试超出范围的索引
	result, ok = ql.Get(10)
	if ok || result != nil {
		t.Errorf("Expected (nil, false) for out of range index, got (%v, %v)", result, ok)
	}
	
	result, ok = ql.Get(-10)
	if ok || result != nil {
		t.Errorf("Expected (nil, false) for out of range negative index, got (%v, %v)", result, ok)
	}
}

func TestQuickListRange(t *testing.T) {
	ql := NewQuickList()
	
	// 测试空列表
	result := ql.Range(0, 2)
	if result != nil {
		t.Errorf("Expected nil for empty list, got %v", result)
	}
	
	// 添加一些值
	for i := 0; i < 5; i++ {
		ql.PushBack([]byte(string(rune('a'+i))))
	}
	
	// 测试正常范围
	result = ql.Range(0, 2)
	if len(result) != 3 {
		t.Fatalf("Expected 3 elements, got %d", len(result))
	}
	if !bytes.Equal(result[0], []byte("a")) ||
	   !bytes.Equal(result[1], []byte("b")) ||
	   !bytes.Equal(result[2], []byte("c")) {
		t.Errorf("Expected ['a', 'b', 'c'], got %v", result)
	}
	
	// 测试负索引
	result = ql.Range(-3, -1)
	if len(result) != 3 {
		t.Fatalf("Expected 3 elements, got %d", len(result))
	}
	if !bytes.Equal(result[0], []byte("c")) ||
	   !bytes.Equal(result[1], []byte("d")) ||
	   !bytes.Equal(result[2], []byte("e")) {
		t.Errorf("Expected ['c', 'd', 'e'], got %v", result)
	}
	
	// 测试边界情况
	result = ql.Range(0, 10) // 超出范围
	expectedLen := 5
	if len(result) != expectedLen {
		t.Errorf("Expected %d elements, got %d", expectedLen, len(result))
	}
	
	// 测试起始位置大于结束位置
	result = ql.Range(3, 2)
	if result != nil {
		t.Errorf("Expected nil when start > stop, got %v", result)
	}
}

func TestQuickListSet(t *testing.T) {
	ql := NewQuickList()
	
	// 测试设置不存在的位置
	ok := ql.Set(0, []byte("test"))
	if ok {
		t.Error("Expected false when setting non-existent index, got true")
	}
	
	// 添加一些值
	ql.PushBack([]byte("first"))
	ql.PushBack([]byte("second"))
	ql.PushBack([]byte("third"))
	
	// 测试设置存在的值
	newVal := []byte("modified")
	ok = ql.Set(1, newVal)
	if !ok {
		t.Error("Expected true when setting existing index, got false")
	}
	
	result, exists := ql.Get(1)
	if !exists || !bytes.Equal(result, newVal) {
		t.Errorf("Expected to get 'modified', got %s", result)
	}
	
	// 测试负索引
	ok = ql.Set(-1, []byte("last-modified"))
	if !ok {
		t.Error("Expected true when setting with negative index, got false")
	}
	
	result, exists = ql.Get(2)
	if !exists || !bytes.Equal(result, []byte("last-modified")) {
		t.Errorf("Expected to get 'last-modified', got %s", result)
	}
	
	// 测试超出范围的索引
	ok = ql.Set(10, []byte("out-of-range"))
	if ok {
		t.Error("Expected false when setting out of range index, got true")
	}
}

func TestQuickListRemoveByValue(t *testing.T) {
	ql := NewQuickList()
	
	// 测试从空列表删除
	removed := ql.RemoveByValue(0, []byte("test"))
	if removed != 0 {
		t.Errorf("Expected 0 removed from empty list, got %d", removed)
	}
	
	// 添加一些值，包括重复项
	ql.PushBack([]byte("a"))
	ql.PushBack([]byte("b"))
	ql.PushBack([]byte("a"))
	ql.PushBack([]byte("c"))
	ql.PushBack([]byte("a"))
	
	// 删除所有 "a"
	removed = ql.RemoveByValue(0, []byte("a"))
	if removed != 3 {
		t.Errorf("Expected 3 'a' to be removed, got %d", removed)
	}
	if ql.len != 2 {
		t.Errorf("Expected len to be 2 after removal, got %d", ql.len)
	}
	
	// 验证剩余元素
	result, _ := ql.Get(0)
	if !bytes.Equal(result, []byte("b")) {
		t.Errorf("Expected 'b' at index 0, got %s", result)
	}
	result, _ = ql.Get(1)
	if !bytes.Equal(result, []byte("c")) {
		t.Errorf("Expected 'c' at index 1, got %s", result)
	}
	
	// 添加更多相同的值，测试删除指定数量
	ql.PushBack([]byte("x"))
	ql.PushBack([]byte("x"))
	ql.PushBack([]byte("x"))
	
	// 只删除前两个 "x"
	removed = ql.RemoveByValue(2, []byte("x"))
	if removed != 2 {
		t.Errorf("Expected 2 'x' to be removed, got %d", removed)
	}
	if ql.len != 3 {
		t.Errorf("Expected len to be 3 after removal, got %d", ql.len)
	}
	
	// 验证剩余元素
	result, _ = ql.Get(2)
	if !bytes.Equal(result, []byte("x")) {
		t.Errorf("Expected 'x' at index 2, got %s", result)
	}
}

func TestQuickListSplitNode(t *testing.T) {
	ql := NewQuickList()
	ql.lpMaxSize = 3 // 设置较小的最大尺寸以便测试分割
	
	// 添加超过最大尺寸限制的元素
	for i := 0; i < 10; i++ {
		ql.PushBack([]byte(string(rune('a'+i))))
	}
	
	// 验证长度是否正确
	if ql.len != 10 {
		t.Errorf("Expected len to be 10, got %d", ql.len)
	}
	
	// 验证能否正确访问所有元素
	for i := 0; i < 10; i++ {
		result, ok := ql.Get(i)
		if !ok || !bytes.Equal(result, []byte(string(rune('a'+i)))) {
			t.Errorf("Expected to get %c at index %d, got %s", rune('a'+i), i, result)
		}
	}
}

func TestQuickListEdgeCases(t *testing.T) {
	ql := NewQuickList()
	
	// 测试大量数据
	for i := 0; i < 1000; i++ {
		ql.PushBack([]byte(string(i)))
	}
	
	if ql.len != 1000 {
		t.Errorf("Expected len to be 1000, got %d", ql.len)
	}
	
	// 测试从头部和尾部交替插入
	ql2 := NewQuickList()
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			ql2.PushFront([]byte(string(i)))
		} else {
			ql2.PushBack([]byte(string(i)))
		}
	}
	
	// 验证总长度
	if ql2.len != 100 {
		t.Errorf("Expected len to be 100, got %d", ql2.len)
	}
	
	// 测试连续弹出
	for ql2.len > 0 {
		ql2.PopFront()
	}
	if ql2.len != 0 {
		t.Errorf("Expected len to be 0 after all pops, got %d", ql2.len)
	}
}