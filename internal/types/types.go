package types

import "strings"

// CmdLine 是命令行的别名，例如: set key val -> [][]byte
type CmdLine [][]byte

var writeCommands = map[string]struct{}{
	// string
	"set":    {},
	"setnx":  {},
	"incr":   {},
	"incrby": {},
	"decr":   {},
	"decrby": {},

	// hash
	"hset": {},
	"hdel": {},

	// list
	"lpush": {},
	"rpush": {},
	"lpop":  {},
	"rpop":  {},
	"lset":  {},
	"ltrim": {},
	"lrem":  {},

	// set
	"sadd": {},
	"srem": {},

	// zset
	"zadd": {},
	"zrem": {},

	// key
	"del":    {},
	"expire": {},
	"rename": {},

	// db
	"select":  {},
	"flushdb": {},
}

func (c CmdLine) IsWrite() bool {
	if len(c) == 0 {
		return false
	}
	cmd := strings.ToLower(string(c[0]))
	_, ok := writeCommands[cmd]
	return ok
}

// DataEntity 代表数据库中的数据实体
type DataEntity struct {
	Data interface{} // 实际数据: string, *list.List, *set.Set, etc.
}
