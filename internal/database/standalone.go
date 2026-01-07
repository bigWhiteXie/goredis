package database

import (
	"goredis/internal/resp"
	"strconv"
	"strings"
)

// StandaloneDatabase 持有所有分库
type StandaloneDatabase struct {
	dbs []*DB // 数组，默认 16 个
}

func NewStandaloneDatabase(aofDir string) *StandaloneDatabase {
	database := &StandaloneDatabase{
		dbs: make([]*DB, 16),
	}
	// 初始化 16 个库
	for i := 0; i < 16; i++ {
		database.dbs[i] = MakeDB(i, aofDir)
	}
	return database
}

// Exec 顶层执行入口
func (mdb *StandaloneDatabase) Exec(c resp.Connection, args [][]byte) resp.Reply {
	// 1. 处理特殊命令: SELECT
	// SELECT 属于 Connection 管理层面的命令，需要在这里拦截
	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		return mdb.execSelect(c, args)
	}

	// 2. 获取当前客户端正在使用的 DB index
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbs) {
		return resp.MakeErrReply("ERR DB index is out of range")
	}

	// 3. 路由到具体的 DB 执行
	selectedDB := mdb.dbs[dbIndex]
	return selectedDB.Exec(c, args)
}

// execSelect 处理 SELECT 命令
func (mdb *StandaloneDatabase) execSelect(c resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return resp.MakeArgNumErrReply("select")
	}

	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return resp.MakeErrReply("ERR invalid DB index")
	}
	if index >= 16 || index < 0 {
		return resp.MakeErrReply("ERR DB index is out of range")
	}

	// 修改 Connection 的状态
	c.SelectDB(index)
	return resp.MakeOkReply()
}

func (mdb *StandaloneDatabase) Close() {
	// 可以在这里做持久化落盘
}

func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
	// 清理逻辑
}
