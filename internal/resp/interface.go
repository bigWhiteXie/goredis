package resp

var (
	CRLF = []byte("\r\n") // RESP 协议的行结束符
)

// Reply 是所有 RESP 协议响应的通用接口
type Reply interface {
	// ToBytes 将响应内容转换为符合 RESP 协议的字节切片，用于写入 TCP 连接
	ToBytes() []byte
}
