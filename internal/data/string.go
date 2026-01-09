package data

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"goredis/internal/resp"
	"goredis/internal/types"
)

type String struct {
	// 如果 isInt=true，则 valInt 有效
	isInt  bool
	valInt int64

	// 普通字符串存这里（二进制安全）
	valRaw []byte
}

func NewStringFromBytes(b []byte) *String {
	// 尝试解析成 int（模拟 Redis 行为）
	if i, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		return &String{
			isInt:  true,
			valInt: i,
		}
	}

	return &String{
		isInt:  false,
		valRaw: append([]byte(nil), b...),
	}
}

func (s *String) Get() []byte {
	if s.isInt {
		return []byte(strconv.FormatInt(s.valInt, 10))
	}
	return s.valRaw
}

func (s *String) Set(b []byte) {
	if i, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		s.isInt = true
		s.valInt = i
		s.valRaw = nil
		return
	}

	s.isInt = false
	s.valRaw = append([]byte(nil), b...)
}

func (s *String) IncrBy(delta int64) (int64, error) {
	if !s.isInt {
		return 0, errors.New("value is not an integer")
	}
	s.valInt += delta
	return s.valInt, nil
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

	str, ok := entity.Data.(*String)
	if !ok {
		return resp.MakeErrReply("ERR wrong type")
	}

	return resp.MakeBulkReply(str.Get())
}
