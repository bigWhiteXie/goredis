package data

import (
	"goredis/internal/resp"
	"goredis/internal/types"
)

// HSET key field value
func execHSet(db types.Database, args [][]byte) resp.Reply {
	if len(args) < 3 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hset' command")
	}

	key := string(args[0])
	field := string(args[1])
	value := args[2]

	entity, exists := db.GetEntity(key)
	var h Hash
	if !exists {
		h = NewRedisHash()
		db.PutEntity(key, &types.DataEntity{Data: h})
	} else {
		var ok bool
		h, ok = entity.Data.(Hash)
		if !ok {
			return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	res := h.HSet(field, value)
	return resp.MakeIntReply(int64(res))
}

// HGET key field
func execHGet(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hget' command")
	}
	key := string(args[0])
	field := string(args[1])

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val, ok := h.HGet(field)
	if !ok {
		return resp.MakeNullBulkReply()
	}
	return resp.MakeBulkReply(val)
}

// HDEL key field [field ...]
func execHDel(db types.Database, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hdel' command")
	}

	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	deleted := h.HDel(fields...)
	return resp.MakeIntReply(int64(deleted))
}

// HEXISTS key field
func execHExists(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hexists' command")
	}

	key := string(args[0])
	field := string(args[1])

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if h.HExists(field) {
		return resp.MakeIntReply(1)
	}
	return resp.MakeIntReply(0)
}

// HLEN key
func execHLEN(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hlen' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeIntReply(int64(h.HLEN()))
}

// HKEYS key
func execHKeys(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hkeys' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	keys := h.HKeys()
	replies := make([][]byte, 0, len(key))
	for _, key := range keys {
		replies = append(replies, []byte(key))
	}
	return resp.MakeMultiBulkReply(replies)
}

// HVALS key
func execHVals(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hvals' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	vals := h.HVals()
	return resp.MakeMultiBulkReply(vals)
}

// HGETALL key
func execHGetAll(db types.Database, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hgetall' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	res := make([][]byte, 0)
	h.HGetAll() // map[string][]byte
	for k, v := range h.HGetAll() {
		res = append(res, []byte(k))
		res = append(res, v)
	}

	return resp.MakeMultiBulkReply(res)
}

// HMSET key field1 val1 [field2 val2 ...]
func execHMSet(db types.Database, args [][]byte) resp.Reply {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hmset' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	var h Hash
	if !exists {
		h = NewRedisHash()
		db.PutEntity(key, &types.DataEntity{Data: h})
	} else {
		var ok bool
		h, ok = entity.Data.(Hash)
		if !ok {
			return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		val := args[i+1]
		h.HSet(field, val)
	}

	return resp.MakeOkReply()
}

// HMGET key field1 [field2 ...]
func execHMGet(db types.Database, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'hmget' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		// 返回对应数量的 nil
		res := make([][]byte, len(args)-1)
		for i := range res {
			res[i] = nil
		}
		return resp.MakeMultiBulkReply(res)
	}

	h, ok := entity.Data.(Hash)
	if !ok {
		return resp.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	res := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		val, _ := h.HGet(string(args[i]))
		if val == nil {
			res[i-1] = nil
		} else {
			res[i-1] = val
		}
	}

	return resp.MakeMultiBulkReply(res)
}
