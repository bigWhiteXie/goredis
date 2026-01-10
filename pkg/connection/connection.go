package connection

type Connection interface {
	// 写数据到客户端
	Write([]byte) (int, error)

	// 关闭连接
	Close() error

	// 连接是否已关闭
	IsClosed() bool

	// 当前选中的 DB
	GetDBIndex() int
	SelectDB(int)

	// 获取客户端地址（用于日志、ACL）
	RemoteAddr() string

	IsSlave() bool
	SetSlave()
}
