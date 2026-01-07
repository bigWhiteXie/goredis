package protocol

// Message 表示一个 RESP 消息
type Message struct {
	Data []byte
}

// Encode 编码为 RESP 字节流
func (m *Message) Encode() []byte {
	return m.Data // TODO: 实现编码逻辑
}