package data

import "goredis/pkg/datastruct"

type Hash interface {
	// 设置字段，如果字段不存在则新增，返回新增字段数量
	HSet(field string, value []byte) int

	// 获取字段
	HGet(field string) ([]byte, bool)

	// 删除字段，返回删除字段数量
	HDel(fields ...string) int

	// 检查字段是否存在
	HExists(field string) bool

	// 返回字段数量
	HLEN() int

	// 返回所有字段
	HKeys() []string

	// 返回所有值
	HVals() [][]byte

	// 返回所有字段->值
	HGetAll() map[string][]byte
}

var _ Hash = &RedisHash{}

type RedisHash struct {
	data datastruct.Dict
}

func NewRedisHash() *RedisHash {
	return &RedisHash{
		data: datastruct.MakeConcurrent(32), // 分片数量可调
	}
}

func (h *RedisHash) HSet(field string, value []byte) int {
	res := h.data.Put(field, append([]byte(nil), value...))
	return res // 1 = 新增字段，0 = 更新字段
}

func (h *RedisHash) HGet(field string) ([]byte, bool) {
	v, ok := h.data.Get(field)
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

func (h *RedisHash) HDel(fields ...string) int {
	deleted := 0
	for _, f := range fields {
		if h.data.Remove(f) > 0 {
			deleted++
		}
	}
	return deleted
}

func (h *RedisHash) HExists(field string) bool {
	_, ok := h.data.Get(field)
	return ok
}

func (h *RedisHash) HLEN() int {
	return h.data.Len()
}

func (h *RedisHash) HKeys() []string {
	return h.data.Keys()
}

func (h *RedisHash) HVals() [][]byte {
	vals := make([][]byte, 0, h.HLEN())
	h.data.ForEach(func(k string, v interface{}) bool {
		vals = append(vals, v.([]byte))
		return true
	})
	return vals
}

func (h *RedisHash) HGetAll() map[string][]byte {
	m := make(map[string][]byte, h.HLEN())
	h.data.ForEach(func(k string, v interface{}) bool {
		m[k] = v.([]byte)
		return true
	})
	return m
}
