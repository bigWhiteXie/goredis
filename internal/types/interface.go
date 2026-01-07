package types

import (
	"goredis/internal/resp"
	"time"
)

// Database 接口定义了 DB 结构体的所有公开方法
type Database interface {
	// GetEntity 从数据库获取数据实体
	GetEntity(key string) (*DataEntity, bool)

	// PutEntity 将数据实体存入数据库
	PutEntity(key string, entity *DataEntity) int

	// PutIfExists 仅当键存在时更新
	PutIfExists(key string, entity *DataEntity) int

	// PutIfAbsent 仅当键不存在时插入
	PutIfAbsent(key string, entity *DataEntity) int

	// Remove 删除指定键
	Remove(key string)

	// Exec 在数据库中执行命令
	Exec(c resp.Connection, cmdLine [][]byte) resp.Reply

	// SetExpire 设置键的过期时间
	SetExpire(key string, expireTime time.Time)

	// IsExpired 检查键是否已过期
	IsExpired(key string) bool

	// StartExpireTask 启动过期任务
	StartExpireTask()

	// DeleteTTL 删除键的过期时间
	DeleteTTL(key string)
}
