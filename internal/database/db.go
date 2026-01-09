package database

import (
	"goredis/internal/command"
	"goredis/internal/persistant"
	"goredis/internal/resp"
	"goredis/internal/types"
	"goredis/pkg/datastruct"
	"time"

	"strings"
)

// DB 代表每一个单独的数据库 (如 db0, db1...)
type DB struct {
	index  int             // 数据库编号
	data   datastruct.Dict // 核心数据存储 (Key -> types.DataEntity)
	ttlMap datastruct.Dict // 过期时间存储 (Key -> time.Time) - 对标 Redis 的 expires

	aofHandler *persistant.AOFHandler
}

func MakeDB(index int, dir string) *DB {
	aofHandler, err := persistant.NewAOFHandler(dir, index)
	if err != nil {
		panic(err)
	}
	db := &DB{
		index:      index,
		data:       datastruct.MakeConcurrent(1024),
		ttlMap:     datastruct.MakeConcurrent(1024),
		aofHandler: aofHandler,
	}

	if aofHandler.HasData() {
		if err := db.loadAOF(); err != nil {
			panic(err)
		}
	}

	db.StartExpireTask()

	return db
}

func (db *DB) loadAOF() error {
	return db.aofHandler.Load(func(cmd types.CmdLine) {
		// FakeConn，避免再次写 AOF
		conn := resp.NewFakeConnection(db.index)
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

// PutIfExists 仅当存在时更新
func (db *DB) PutIfExists(key string, entity *types.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent 仅当不存在时写入 (SETNX)
func (db *DB) PutIfAbsent(key string, entity *types.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove 删除 Key
func (db *DB) Remove(key string) bool {
	db.ttlMap.Remove(key) // 别忘了删除 TTL
	return db.data.Remove(key) == 1
}

// Exec 在单个 DB 中执行命令
// 实际逻辑是：根据 command name 查表找到对应的 ExecFunc 并调用
func (db *DB) Exec(c resp.Connection, cmdLine [][]byte) resp.Reply {
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
		db.aofHandler.AddAOF(cmdLine)
	}

	return reply
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

func validateArity(arity int, cmdLine [][]byte) bool {
	n := len(cmdLine)

	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}
