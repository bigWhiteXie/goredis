package resp

var (
	CRLF = []byte("\r\n") // RESP 协议的行结束符
)

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
}

// Reply 是所有 RESP 协议响应的通用接口
type Reply interface {
	// ToBytes 将响应内容转换为符合 RESP 协议的字节切片，用于写入 TCP 连接
	ToBytes() []byte
}
