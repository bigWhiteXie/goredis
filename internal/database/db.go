package database

import (
	"strings"
	"time"

	"goredis/internal/command"
	"goredis/internal/persistant"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/connection"
	"goredis/pkg/datastruct"
)

const (
	aofRewriteMinSize    = 64 * 1024 * 1024 // 64MB
	aofRewritePercentage = 25               // 增长 25%
	aofCheckInterval     = 10 * time.Second
)

// DB 代表每一个单独的数据库 (如 db0, db1...)
type DB struct {
	index  int             // 数据库编号
	data   datastruct.Dict // 核心数据存储 (Key -> types.DataEntity)
	ttlMap datastruct.Dict // 过期时间存储 (Key -> time.Time) - 对标 Redis 的 expires

	aofHandler *persistant.AOFHandler
}

func MakeDB(index int, aofHandler *persistant.AOFHandler) *DB {
	db := &DB{
		index:      index,
		data:       datastruct.MakeConcurrent(1024),
		ttlMap:     datastruct.MakeConcurrent(1024),
		aofHandler: aofHandler,
	}

	if aofHandler.HasData() {
		if err := db.LoadAOF(); err != nil {
			panic(err)
		}
	}

	db.StartExpireTask()
	db.startAOFRewriteChecker()
	return db
}

func (db *DB) LoadAOF() error {
	return db.aofHandler.Load(func(cmd types.CmdLine) {
		// FakeConn，避免再次写 AOF
		conn := connection.NewAOFConnection(db.index)
		db.Exec(conn, cmd)
	})
}

// GetEntity 从 dict 获取 types.DataEntity
func (db *DB) GetEntity(key string) (*types.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	// 检查是否过期
	if db.IsExpired(key) {
		db.Remove(key)
		return nil, false
	}
	entity, _ := raw.(*types.DataEntity)
	return entity, true
}

// PutEntity 将 types.DataEntity 存入 dict
func (db *DB) PutEntity(key string, entity *types.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) DeleteTTL(key string) {
	db.ttlMap.Remove(key)
}

// Remove 删除 Key
func (db *DB) Remove(key string) bool {
	db.ttlMap.Remove(key) // 别忘了删除 TTL
	return db.data.Remove(key) == 1
}

// Exec 在单个 DB 中执行命令
// 实际逻辑是：根据 command name 查表找到对应的 ExecFunc 并调用
func (db *DB) Exec(c connection.Connection, cmdLine [][]byte) resp.Reply {
	// 1. 获取命令名称 (如 "SET")
	cmdName := strings.ToLower(string(cmdLine[0]))

	cmd, ok := command.GetCmd(cmdName)
	if !ok {
		return resp.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	// 3. 校验参数个数 (Arity Check)
	if !validateArity(cmd.Arity, cmdLine) {
		return resp.MakeArgNumErrReply(cmdName)
	}

	// 4. 执行具体函数
	reply := cmd.Executor(db, cmdLine[1:])
	if !resp.IsErrorReply(reply) {
		if _, ok := c.(*connection.AOFConnection); !ok {
			db.aofHandler.AddAOF(cmdLine)
		}
	}

	return reply
}

func (db *DB) GetDBIndex() int {
	return db.index
}

func (db *DB) ForEach(handler func(key string, entity types.RedisData)) {
	db.data.ForEach(func(key string, data interface{}) bool {
		handler(key, data.(types.RedisData))
		return true
	})
}

func (db *DB) SetExpire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
}

func (db *DB) IsExpired(key string) bool {
	raw, ok := db.ttlMap.Get(key)
	if !ok {
		return false // 没有 TTL
	}

	expireTime, ok := raw.(time.Time)
	if !ok {
		return false
	}

	if time.Now().After(expireTime) {
		return true
	}
	return false
}

func (db *DB) StartExpireTask() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			db.activeExpire()
		}
	}()
}

func (db *DB) activeExpire() {
	const sampleSize = 20 // 每轮抽样 key 数（Redis 默认是 20）

	keys := db.ttlMap.RandomKeys(sampleSize)
	if len(keys) == 0 {
		return
	}

	now := time.Now()

	for _, key := range keys {
		raw, ok := db.ttlMap.Get(key)
		if !ok {
			continue
		}

		expireAt, ok := raw.(time.Time)
		if !ok {
			continue
		}

		if now.After(expireAt) {
			db.Remove(key)
		}
	}
}

func (db *DB) GetExpireTime(key string) (time.Time, bool) {
	val, ok := db.ttlMap.Get(key)
	if !ok {
		return time.Now(), false
	}

	return val.(time.Time), true
}

func (db *DB) Clear() {
	db.data = datastruct.MakeConcurrent(1024)
	db.ttlMap = datastruct.MakeConcurrent(1024)
}

func (db *DB) startAOFRewriteChecker() {
	go func() {
		ticker := time.NewTicker(aofCheckInterval)
		defer ticker.Stop()

		var lastRewriteSize int64

		for range ticker.C {
			aof := db.aofHandler
			if aof == nil {
				continue
			}

			size, err := aof.LogSize()
			if err != nil {
				continue
			}

			// 小于最小 rewrite 大小，不处理
			if size < aofRewriteMinSize {
				continue
			}

			// 首次记录基线
			if lastRewriteSize == 0 {
				lastRewriteSize = size
				continue
			}

			// 判断增长比例
			growth := (size - lastRewriteSize) * 100 / lastRewriteSize
			if growth < aofRewritePercentage {
				continue
			}

			// 触发 rewrite
			db.aofHandler.Rewrite(db.Clone())
		}
	}()
}

func (db *DB) Clone() *DB {
	// 创建新的 Dict
	newData := datastruct.MakeConcurrent(db.data.Len())
	newTTL := datastruct.MakeConcurrent(db.ttlMap.Len())

	// 拷贝 data
	db.data.ForEach(func(key string, val interface{}) bool {
		// val 一般是 types.DataEntity
		data := val.(types.Cloneable)
		newData.Put(key, data.Clone())
		return true
	})

	// 拷贝 ttlMap
	db.ttlMap.ForEach(func(key string, val interface{}) bool {
		// val 是 time.Time
		newTTL.Put(key, val)
		return true
	})

	return &DB{
		index:  db.index,
		data:   newData,
		ttlMap: newTTL,
	}
}

// 校验参数数量
func validateArity(arity int, cmdLine [][]byte) bool {
	n := len(cmdLine)

	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}
