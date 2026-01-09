package command

import (
	"errors"
	"goredis/internal/data"
	"goredis/internal/resp"
	"goredis/internal/types"
	"strconv"
)

func execSAdd(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	s, err := getOrCreateSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}

	added := 0
	for _, member := range args[1:] {
		if s.Add(member) {
			added++
		}
	}

	return resp.MakeIntReply(int64(added))
}

func execSRem(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists {
		return resp.MakeIntReply(0)
	}

	removed := 0
	for _, member := range args[1:] {
		if s.Remove(member) {
			removed++
		}
	}

	if s.Len() == 0 {
		db.Remove(key)
	}

	return resp.MakeIntReply(int64(removed))
}

func execSIsMember(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	member := args[1]

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists {
		return resp.MakeIntReply(0)
	}

	if s.Contains(member) {
		return resp.MakeIntReply(1)
	}
	return resp.MakeIntReply(0)
}

func execSCard(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists {
		return resp.MakeIntReply(0)
	}

	return resp.MakeIntReply(int64(s.Len()))
}

func execSMembers(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists {
		return resp.MakeNullBulkReply()
	}

	return resp.MakeMultiBulkReply(s.Members())
}

func execSRandMember(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	count := 1
	if len(args) == 2 {
		n, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return resp.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = n
	}

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists || s.Len() == 0 {
		return resp.MakeNullBulkReply()
	}

	if count == 1 {
		v, _ := s.Random()
		return resp.MakeBulkReply(v)
	}

	// 简化实现（可重复）
	res := make([][]byte, 0, count)
	for i := 0; i < abs(count); i++ {
		v, ok := s.Random()
		if !ok {
			break
		}
		res = append(res, v)
	}

	return resp.MakeMultiBulkReply(res)
}

func execSPop(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	s, exists, err := getSet(db, key)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if !exists {
		return resp.MakeNullBulkReply()
	}

	v, ok := s.Pop()
	if !ok {
		return resp.MakeNullBulkReply()
	}

	if s.Len() == 0 {
		db.Remove(key)
	}

	return resp.MakeBulkReply(v)
}

func execSUnion(db types.Database, args [][]byte) resp.Reply {
	sets, err := collectSets(db, args)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}

	result := data.NewSet()
	for _, s := range sets {
		for _, m := range s.Members() {
			result.Add(m)
		}
	}

	return resp.MakeMultiBulkReply(result.Members())
}

func execSInter(db types.Database, args [][]byte) resp.Reply {
	sets, err := collectSets(db, args)
	if err != nil {
		return resp.MakeErrReply(err.Error())
	}
	if len(sets) == 0 {
		return resp.MakeNullBulkReply()
	}

	base := sets[0]
	result := data.NewSet()

	for _, m := range base.Members() {
		ok := true
		for i := 1; i < len(sets); i++ {
			if !sets[i].Contains(m) {
				ok = false
				break
			}
		}
		if ok {
			result.Add(m)
		}
	}

	return resp.MakeMultiBulkReply(result.Members())
}

func collectSets(db types.Database, keys [][]byte) ([]*data.SetObject, error) {
	sets := make([]*data.SetObject, 0, len(keys))
	for _, k := range keys {
		s, exists, err := getSet(db, string(k))
		if err != nil {
			return nil, err
		}
		if exists {
			sets = append(sets, s)
		}
	}
	return sets, nil
}

func getOrCreateSet(db types.Database, key string) (*data.SetObject, error) {
	entity, exists := db.GetEntity(key)
	if !exists {
		s := data.NewSet()
		db.PutEntity(key, &types.DataEntity{Data: s})
		return s, nil
	}
	s, ok := entity.Data.(*data.SetObject)
	if !ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return s, nil
}

func getSet(db types.Database, key string) (*data.SetObject, bool, error) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, false, nil
	}
	s, ok := entity.Data.(*data.SetObject)
	if !ok {
		return nil, false, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return s, true, nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
