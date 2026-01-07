package types

// CmdLine 是命令行的别名，例如: set key val -> [][]byte
type CmdLine = [][]byte

// DataEntity 代表数据库中的数据实体
type DataEntity struct {
	Data interface{} // 实际数据: string, *list.List, *set.Set, etc.
}
