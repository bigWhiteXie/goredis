package datastruct

type HashSet struct {
	dict Dict
}

func NewHashSet() *HashSet {
	return &HashSet{
		dict: MakeConcurrent(ShardCount),
	}
}

func (s *HashSet) Add(member []byte) bool {
	key := string(member)
	return s.dict.PutIfAbsent(key, struct{}{}) == 1
}

func (s *HashSet) Remove(member []byte) bool {
	key := string(member)
	return s.dict.Remove(key) == 1
}

func (s *HashSet) Contains(member []byte) bool {
	key := string(member)
	_, exists := s.dict.Get(key)
	return exists
}

func (s *HashSet) Len() int {
	return s.dict.Len()
}

func (s *HashSet) Members() [][]byte {
	keys := s.dict.Keys()
	res := make([][]byte, len(keys))
	for i, k := range keys {
		res[i] = []byte(k)
	}
	return res
}

func (s *HashSet) Random() ([]byte, bool) {
	keys := s.dict.RandomKeys(1)
	if len(keys) == 0 {
		return nil, false
	}
	return []byte(keys[0]), true
}

func (s *HashSet) Pop() ([]byte, bool) {
	keys := s.dict.RandomKeys(1)
	if len(keys) == 0 {
		return nil, false
	}
	key := keys[0]
	s.dict.Remove(key)
	return []byte(key), true
}
