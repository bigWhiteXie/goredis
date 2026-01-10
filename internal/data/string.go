package data

import (
	"errors"
	"goredis/internal/common"
	"goredis/internal/types"
	"strconv"
)

type String interface {
	types.RedisData

	// GET
	Get() []byte

	// SET（覆盖写）
	Set(val []byte)

	// INCR / INCRBY / DECRBY
	// 如果不是整数，返回 error（Redis 行为）
	IncrBy(delta int64) (int64, error)
}

var _ String = &SimpleString{}

type SimpleString struct {
	// 如果 isInt=true，则 valInt 有效
	isInt  bool
	valInt int64

	// 普通字符串存这里（二进制安全）
	valRaw []byte
}

func NewSimpleString(isInt bool, val int64) *SimpleString {
	return &SimpleString{
		isInt:  isInt,
		valInt: val,
	}
}
func NewStringFromBytes(b []byte) *SimpleString {
	// 尝试解析成 int（模拟 Redis 行为）
	if i, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		return &SimpleString{
			isInt:  true,
			valInt: i,
		}
	}

	return &SimpleString{
		isInt:  false,
		valRaw: append([]byte(nil), b...),
	}
}

func (s *SimpleString) Get() []byte {
	if s.isInt {
		return []byte(strconv.FormatInt(s.valInt, 10))
	}
	return s.valRaw
}

func (s *SimpleString) Set(b []byte) {
	if i, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		s.isInt = true
		s.valInt = i
		s.valRaw = nil
		return
	}

	s.isInt = false
	s.valRaw = append([]byte(nil), b...)
}

func (s *SimpleString) IncrBy(delta int64) (int64, error) {
	if !s.isInt {
		return 0, errors.New("value is not an integer")
	}
	s.valInt += delta
	return s.valInt, nil
}

func (s *SimpleString) ToWriteCmdLine(key string) [][]byte {
	return [][]byte{
		[]byte("set"),
		[]byte(key),
		s.Get(),
	}
}

func (s *SimpleString) Clone() interface{} {
	return &SimpleString{
		isInt:  s.isInt,
		valInt: s.valInt,
		valRaw: common.CloneBytes(s.valRaw),
	}
}
