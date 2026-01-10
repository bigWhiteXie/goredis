package command

import (
	"bytes"
	"strconv"

	"goredis/internal/data"
	"goredis/internal/resp"
	"goredis/internal/types"
)

// ZADD key [NX|XX] [CH] score member [score member ...]
func execZAdd(db types.Database, args [][]byte) resp.Reply {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'zadd' command")
	}

	key := string(args[0])
	zset := getOrCreateZSet(db, key)

	var nx, xx bool
	scoreMemberStart := 1

	// 解析选项
	for i := 1; i < len(args); i++ {
		opt := bytes.ToLower(args[i])
		if bytes.Equal(opt, []byte("nx")) {
			nx = true
		} else if bytes.Equal(opt, []byte("xx")) {
			xx = true
		} else {
			// 第一个非选项位置即为 score
			scoreMemberStart = i
			break
		}
	}

	added := 0
	for i := scoreMemberStart; i < len(args); i += 2 {
		scoreStr := string(args[i])
		member := args[i+1]

		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return resp.MakeErrReply("ERR value is not a valid float")
		}

		added += zset.ZAdd(nx, xx, score, member)
	}

	db.PutEntity(key, &types.DataEntity{Data: zset})
	return resp.MakeIntReply(int64(added))
}

// ZCARD key
func execZCard(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}

	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}
	return resp.MakeIntReply(int64(zs.ZCard()))
}

// ZSCORE key member
func execZScore(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	member := args[1]

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	score, ok := zs.ZScore(member)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
}

// ZRANK key member
func execZRank(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	member := args[1]

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	rank, ok := zs.ZRank(member)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeIntReply(int64(rank))
}

// ZREVRANK key member
func execZRevRank(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	member := args[1]

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	rank, ok := zs.ZRevRank(member)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeIntReply(int64(rank))
}

// ZRANGE key start stop [WITHSCORES]
func execZRange(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	start, err1 := strconv.Atoi(string(args[1]))
	stop, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}

	withScores := false
	if len(args) > 3 && bytes.Equal(bytes.ToLower(args[3]), []byte("withscores")) {
		withScores = true
	}

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	items := zs.ZRange(start, stop, withScores)
	return resp.MakeMultiBulkReply(items)
}

// ZREVRANGE key start stop [WITHSCORES]
func execZRevRange(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	start, err1 := strconv.Atoi(string(args[1]))
	stop, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}

	withScores := false
	if len(args) > 3 && bytes.Equal(bytes.ToLower(args[3]), []byte("withscores")) {
		withScores = true
	}

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	items := zs.ZRevRange(start, stop, withScores)
	return resp.MakeMultiBulkReply(items)
}

// ZCOUNT key min max
func execZCount(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	min, err1 := strconv.ParseFloat(string(args[1]), 64)
	max, err2 := strconv.ParseFloat(string(args[2]), 64)
	if err1 != nil || err2 != nil {
		return resp.MakeErrReply("ERR value is not a float")
	}

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	count := zs.ZCount(min, max)
	return resp.MakeIntReply(int64(count))
}

// ZREM key member [member ...]
func execZRem(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	if len(args) < 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'zrem' command")
	}

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeIntReply(0)
	}
	zs, ok := entity.Data.(*data.ZSet)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	removed := 0
	for _, member := range args[1:] {
		removed += zs.ZRem(member)
	}

	return resp.MakeIntReply(int64(removed))
}

// helper: 获取或创建 data.ZSet
func getOrCreateZSet(db types.Database, key string) *data.ZSet {
	entity, ok := db.GetEntity(key)
	if ok {
		if zs, ok2 := entity.Data.(*data.ZSet); ok2 {
			return zs
		}
	}
	zs := data.NewZSet()
	db.PutEntity(key, &types.DataEntity{Data: zs})
	return zs
}
