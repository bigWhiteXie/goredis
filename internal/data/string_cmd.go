package data

import (
	"goredis/internal/resp"
	"goredis/internal/types"
	"strconv"
	"strings"
	"time"
)

func getString(db types.Database, key string) (*SimpleString, bool, resp.Reply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, false, nil
	}
	str, ok := entity.Data.(*SimpleString)
	if !ok {
		return nil, false, resp.MakeErrReply("ERR wrong type")
	}
	return str, true, nil
}

func execIncr(db types.Database, args [][]byte) resp.Reply {
	return execIncrBy(db, [][]byte{args[0], []byte("1")})
}

func execDecr(db types.Database, args [][]byte) resp.Reply {
	return execIncrBy(db, [][]byte{args[0], []byte("-1")})
}

func execIncrBy(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}

	str, exists, errReply := getString(db, key)
	if errReply != nil {
		return errReply
	}

	// 不存在则当作 0
	if !exists {
		str = NewStringFromBytes([]byte("0"))
		db.PutEntity(key, &types.DataEntity{Data: str})
	}

	val, err := str.IncrBy(delta)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}

	return resp.MakeIntReply(val)
}

func execAppend(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	appendVal := args[1]

	str, exists, errReply := getString(db, key)
	if errReply != nil {
		return errReply
	}

	if !exists {
		str = NewStringFromBytes(appendVal)
		db.PutEntity(key, &types.DataEntity{Data: str})
		return resp.MakeIntReply(int64(len(appendVal)))
	}

	// 如果当前是 int，先转成 raw
	cur := str.Get()
	newVal := append(cur, appendVal...)
	str.Set(newVal)

	return resp.MakeIntReply(int64(len(newVal)))
}

func execStrLen(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	str, exists, errReply := getString(db, key)
	if errReply != nil {
		return errReply
	}
	if !exists {
		return resp.MakeIntReply(0)
	}

	return resp.MakeIntReply(int64(len(str.Get())))
}

func execSetNX(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]

	if _, exists := db.GetEntity(key); exists {
		return resp.MakeIntReply(0)
	}

	str := NewStringFromBytes(value)
	db.PutEntity(key, &types.DataEntity{Data: str})
	db.DeleteTTL(key)

	return resp.MakeIntReply(1)
}

func execDecrBy(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer or out of range")
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		str := &SimpleString{isInt: true, valInt: -delta}
		db.PutEntity(key, &types.DataEntity{Data: str})
		return resp.MakeIntReply(-delta)
	}

	str, ok := entity.Data.(*SimpleString)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	val, err := str.IncrBy(-delta)
	if err != nil {
		return resp.MakeErrReply("ERR value is not an integer")
	}

	return resp.MakeIntReply(val)
}

func execSet(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]

	var (
		useNX    bool
		expireAt time.Time
		hasTTL   bool
	)

	// 1. 解析参数
	for i := 2; i < len(args); i++ {
		option := strings.ToLower(string(args[i]))
		switch option {
		case "ex":
			if i+1 >= len(args) {
				return resp.MakeErrReply("ERR wrong number of arguments for 'set' command")
			}
			seconds, err := strconv.Atoi(string(args[i+1]))
			if err != nil || seconds <= 0 {
				return resp.MakeErrReply("ERR invalid expire time")
			}
			expireAt = time.Now().Add(time.Duration(seconds) * time.Second)
			hasTTL = true
			i++
		case "nx":
			useNX = true
		default:
			return resp.MakeErrReply("ERR unknown option")
		}
	}

	// 2. NX 语义判断（在写之前）
	if useNX {
		if _, exists := db.GetEntity(key); exists {
			// Redis 行为：返回 nil bulk reply
			return resp.MakeNullBulkReply()
		}
	}
	str := NewStringFromBytes(value)
	// 3. 写入数据（覆盖写会清理旧 TTL）
	db.PutEntity(key, &types.DataEntity{Data: str})
	db.DeleteTTL(key)

	// 4. 设置过期时间
	if hasTTL {
		db.SetExpire(key, expireAt)
	}

	return resp.MakeOkReply()
}

func execGet(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])

	entity, ok := db.GetEntity(key)
	if !ok {
		return resp.MakeNullBulkReply()
	}

	str, ok := entity.Data.(*SimpleString)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	return resp.MakeBulkReply(str.Get())
}

func execMSet(db types.Database, args [][]byte) resp.Reply {
	if len(args)%2 != 0 {
		return resp.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	for i := 0; i < len(args); i += 2 {
		key := string(args[i])
		val := args[i+1]

		db.PutEntity(key, &types.DataEntity{
			Data: NewStringFromBytes(val),
		})
		db.DeleteTTL(key)
	}

	return resp.MakeOkReply()
}

func execMGet(db types.Database, args [][]byte) resp.Reply {
	result := make([][]byte, len(args))

	for i, arg := range args {
		key := string(arg)
		entity, exists := db.GetEntity(key)
		if !exists {
			result[i] = nil
			continue
		}

		str, ok := entity.Data.(*SimpleString)
		if !ok {
			result[i] = nil
			continue
		}
		result[i] = str.Get()
	}

	return resp.MakeMultiBulkReply(result)
}

func execDel(db types.Database, args [][]byte) resp.Reply {
	deleted := int64(0)
	for _, arg := range args {
		key := string(arg)
		if db.Remove(key) {
			deleted++
		}
	}
	return resp.MakeIntReply(deleted)
}
