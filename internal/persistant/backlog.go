package persistant

import (
	"sync"
)

type ReplBacklog struct {
	buf  []byte
	size int64

	start int64 // buf 中对应的全局 offset 起点
	end   int64 // 当前写入到的全局 offset（不包含）

	idx int64 // 环形写指针
	mu  sync.Mutex
}

func NewReplBacklog(size int64, startOffset int64) *ReplBacklog {
	return &ReplBacklog{
		buf:   make([]byte, size),
		size:  size,
		start: startOffset,
		end:   startOffset,
		idx:   0,
	}
}

func (rb *ReplBacklog) Append(data []byte) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for _, b := range data {
		rb.buf[rb.idx] = b
		rb.idx = (rb.idx + 1) % rb.size

		// backlog 满了，start 向前推进
		if rb.end-rb.start >= rb.size {
			rb.start++
		}

		rb.end++
	}
}

func (rb *ReplBacklog) CanServe(offset int64) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return offset >= rb.start && offset < rb.end
}

func (rb *ReplBacklog) ReadFrom(offset int64) []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if offset < rb.start || offset >= rb.end {
		return nil
	}

	length := rb.end - offset
	data := make([]byte, length)

	// 计算 offset 对应的物理起始位置
	// 当前写入位置 rb.idx 指向 rb.end 的下一个位置
	// 所以 rb.end 对应的物理位置是 (rb.idx - 1 + rb.size) % rb.size
	// 那么 offset 对应的位置 = (rb.idx - (rb.end - offset) + rb.size) % rb.size
	startIdx := (rb.idx - (rb.end - offset) + rb.size) % rb.size

	for i := int64(0); i < length; i++ {
		pos := (startIdx + i) % rb.size
		data[i] = rb.buf[pos]
	}

	return data
}

func (rb *ReplBacklog) GetStartOffset() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.start
}
