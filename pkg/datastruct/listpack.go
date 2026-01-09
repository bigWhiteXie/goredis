package datastruct

type ListPack struct {
	data [][]byte
}

func NewListPack(size int) *ListPack {
	return &ListPack{
		data: make([][]byte, 0, size),
	}
}

func (lp *ListPack) PushFront(val []byte) {
	// prepend
	lp.data = append([][]byte{val}, lp.data...)
}

func (lp *ListPack) PushBack(val []byte) {
	lp.data = append(lp.data, val)
}

func (lp *ListPack) PopFront() []byte {
	if len(lp.data) == 0 {
		return nil
	}
	val := lp.data[0]
	lp.data = lp.data[1:]
	return val
}

func (lp *ListPack) PopBack() []byte {
	n := len(lp.data)
	if n == 0 {
		return nil
	}
	val := lp.data[n-1]
	lp.data = lp.data[:n-1]
	return val
}

func (lp *ListPack) Get(index int) ([]byte, bool) {
	n := len(lp.data)
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return nil, false
	}
	return lp.data[index], true
}

func (lp *ListPack) Range(start, stop int) [][]byte {
	n := len(lp.data)
	if n == 0 {
		return nil
	}

	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return nil
	}

	// copy 一份，避免外部修改内部 slice
	res := make([][]byte, stop-start+1)
	copy(res, lp.data[start:stop+1])
	return res
}

func (lp *ListPack) Set(index int, val []byte) bool {
	n := len(lp.data)
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return false
	}
	lp.data[index] = val
	return true
}

// count > 0: 从头删 count 个
// count < 0: 从尾删 |count| 个
// count = 0: 删除所有
func (lp *ListPack) RemoveByValue(count int, val []byte) int {
	removed := 0

	eq := func(a, b []byte) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	if count == 0 {
		dst := lp.data[:0]
		for _, v := range lp.data {
			if eq(v, val) {
				removed++
			} else {
				dst = append(dst, v)
			}
		}
		lp.data = dst
		return removed
	}

	if count > 0 {
		dst := make([][]byte, 0, len(lp.data))
		for _, v := range lp.data {
			if removed < count && eq(v, val) {
				removed++
				continue
			}
			dst = append(dst, v)
		}
		lp.data = dst
		return removed
	}

	// count < 0，从尾部删
	count = -count
	dst := make([][]byte, 0, len(lp.data))
	for i := len(lp.data) - 1; i >= 0; i-- {
		if removed < count && eq(lp.data[i], val) {
			removed++
			continue
		}
		dst = append(dst, lp.data[i])
	}
	// reverse dst
	for i, j := 0, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	lp.data = dst
	return removed
}

func (lp *ListPack) Clear() {
	lp.data = lp.data[:0]
}

func (lp *ListPack) Len() int {
	return len(lp.data)
}
