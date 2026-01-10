package data

import (
	"goredis/internal/common"
	"goredis/internal/types"
	"goredis/pkg/datastruct"
	"strconv"
)

type Set interface {
	types.RedisData

	Add(member []byte) bool
	Remove(member []byte) bool
	Contains(member []byte) bool
	Len() int
	Members() [][]byte
	Random() ([]byte, bool)
	Pop() ([]byte, bool)
}

type Encoding int

const (
	EncIntSet Encoding = iota
	EncHash
)

const maxIntSetEntries = 512 // 对齐 Redis 默认值

var _ Set = &SetObject{}

type SetObject struct {
	encoding Encoding
	is       *datastruct.IntSet
	hs       *datastruct.HashSet
}

func NewSet() *SetObject {
	return &SetObject{
		encoding: EncIntSet,
		is:       datastruct.NewIntSet(),
	}
}

func (s *SetObject) Add(member []byte) bool {
	switch s.encoding {
	case EncIntSet:
		if v, ok := common.ParseInt(member); ok {
			added := s.is.Add(v)
			if s.is.Len() > maxIntSetEntries {
				s.upgradeToHash()
			}
			return added
		}
		// 非整数，必须升级
		s.upgradeToHash()
		return s.hs.Add(member)

	case EncHash:
		return s.hs.Add(member)
	}
	return false
}

func (s *SetObject) Remove(member []byte) bool {
	switch s.encoding {
	case EncIntSet:
		if v, ok := common.ParseInt(member); ok {
			return s.is.Remove(v)
		}
		return false
	case EncHash:
		return s.hs.Remove(member)
	}
	return false
}

func (s *SetObject) Contains(member []byte) bool {
	switch s.encoding {
	case EncIntSet:
		if v, ok := common.ParseInt(member); ok {
			return s.is.Contains(v)
		}
		return false
	case EncHash:
		return s.hs.Contains(member)
	}
	return false
}

func (s *SetObject) Len() int {
	if s.encoding == EncIntSet {
		return s.is.Len()
	}
	return s.hs.Len()
}

func (s *SetObject) Members() [][]byte {
	if s.encoding == EncIntSet {
		vals := s.is.Values()
		res := make([][]byte, len(vals))
		for i, v := range vals {
			res[i] = []byte(strconv.FormatInt(v, 10))
		}
		return res
	}
	return s.hs.Members()
}

func (s *SetObject) upgradeToHash() {
	if s.encoding == EncHash {
		return
	}

	hs := datastruct.NewHashSet()
	for _, v := range s.is.Values() {
		hs.Add([]byte(strconv.FormatInt(v, 10)))
	}

	s.is = nil
	s.hs = hs
	s.encoding = EncHash
}

func (s *SetObject) Random() ([]byte, bool) {
	if s.Len() == 0 {
		return nil, false
	}
	if s.encoding == EncIntSet {
		v, _ := s.is.Random()
		return []byte(strconv.FormatInt(v, 10)), true
	}
	return s.hs.Random()
}

func (s *SetObject) Pop() ([]byte, bool) {
	if s.Len() == 0 {
		return nil, false
	}
	if s.encoding == EncIntSet {
		v, _ := s.is.Pop()
		return []byte(strconv.FormatInt(v, 10)), true
	}
	return s.hs.Pop()
}

func (s *SetObject) ToWriteCmdLine(key string) [][]byte {
	cmdLine := [][]byte{[]byte("sadd"), []byte(key)}
	for _, member := range s.Members() {
		cmdLine = append(cmdLine, member)
	}

	return cmdLine
}

func (s *SetObject) Clone() interface{} {
	ns := NewSet()
	for _, member := range s.Members() {
		ns.Add(common.CloneBytes(member))
	}

	return ns
}
