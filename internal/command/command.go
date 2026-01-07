package command

import (
	"strconv"
	"time"

	"goredis/internal/resp"
	"goredis/internal/types"
)

// ExecFunc 定义每个 Redis 命令的执行函数签名
type ExecFunc func(db types.Database, args [][]byte) resp.Reply

// Command 定义了一个命令的元数据
type Command struct {
	Name     string   // 命令名称
	Executor ExecFunc // 执行函数
	Arity    int      // 参数数量限制 (例如: SET key val 是 3，如果允许不定参数用负数表示)
}

// 全局命令注册表
var cmdTable = make(map[string]*Command)

func RegisterCommand(cmd *Command) {
	cmdTable[cmd.Name] = &Command{
		Name:     cmd.Name,
		Executor: cmd.Executor,
		Arity:    cmd.Arity,
	}
}

func execDel(db types.Database, args [][]byte) resp.Reply {
	deleted := 0

	for _, arg := range args {
		key := string(arg)

		_, exists := db.GetEntity(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}

	return resp.MakeIntReply(int64(deleted))
}

func execExpire(db types.Database, args [][]byte) resp.Reply {
	key := string(args[0])
	seconds, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("ERR invalid expire time")
	}

	_, exists := db.GetEntity(key)
	if !exists {
		return resp.MakeIntReply(0)
	}

	if seconds <= 0 {
		db.Remove(key)
		return resp.MakeIntReply(1)
	}

	expireAt := time.Now().Add(time.Duration(seconds) * time.Second)
	db.SetExpire(key, expireAt)

	return resp.MakeIntReply(1)
}
