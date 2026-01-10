package command

import (
	"strconv"

	"goredis/internal/data"
	"goredis/internal/resp"
	"goredis/internal/types"
)

func execLPush(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	values := args[1:]

	var ql *data.QuickList

	entity, exists := db.GetEntity(key)
	if !exists {
		ql = data.NewQuickList()
		db.PutEntity(key, &types.DataEntity{Data: ql})
	} else {
		var ok bool
		ql, ok = entity.Data.(*data.QuickList)
		if !ok {
			return resp.MakeErrReply("ERR wrong type")
		}
	}

	for _, v := range values {
		ql.PushFront(v)
	}

	return resp.MakeIntReply(int64(ql.Len()))
}

func execRPush(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	values := args[1:]

	var ql *data.QuickList

	entity, exists := db.GetEntity(key)
	if !exists {
		ql = data.NewQuickList()
		db.PutEntity(key, &types.DataEntity{Data: ql})
	} else {
		var ok bool
		ql, ok = entity.Data.(*data.QuickList)
		if !ok {
			return resp.MakeErrReply("ERR wrong type")
		}
	}

	for _, v := range values {
		ql.PushBack(v)
	}

	return resp.MakeIntReply(int64(ql.Len()))
}

func execLPop(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	val := ql.PopFront()
	if val == nil {
		return resp.MakeNullBulkReply()
	}

	return resp.MakeBulkReply(val)
}

func execRPop(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	val := ql.PopBack()
	if val == nil {
		return resp.MakeNullBulkReply()
	}

	return resp.MakeBulkReply(val)
}

func execLLen(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	return resp.MakeIntReply(int64(ql.Len()))
}

func execLIndex(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	val, ok := ql.Get(index)
	if !ok {
		return resp.MakeNullBulkReply()
	}

	return resp.MakeBulkReply(val)
}

func execLSet(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}
	value := args[2]

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeErrReply("ERR no such key")
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	if !ql.Set(index, value) {
		return resp.MakeErrReply("ERR index out of range")
	}

	return resp.MakeOkReply()
}

func execLRange(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	start, _ := strconv.Atoi(string(args[1]))
	stop, _ := strconv.Atoi(string(args[2]))

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeNullBulkReply()
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	values := ql.Range(start, stop)

	replies := make([][]byte, 0, len(values))
	for _, v := range values {
		replies = append(replies, v)
	}

	return resp.MakeMultiBulkReply(replies)
}

func execLRem(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}
	value := args[2]

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	removed := ql.RemoveByValue(count, value)
	return resp.MakeIntReply(int64(removed))
}

func execLTrim(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	start, _ := strconv.Atoi(string(args[1]))
	stop, _ := strconv.Atoi(string(args[2]))

	entity, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeOkReply()
	}

	ql, ok := entity.Data.(*data.QuickList)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	ql.Trim(start, stop)
	return resp.MakeOkReply()
}
