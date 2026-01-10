package resp

import (
	"fmt"
	"strconv"
)

var OkReply = &SimpleStringReply{Status: "OK"}

func MakeOkReply() *SimpleStringReply {
	return OkReply
}

func MakeArgNumErrReply(cmdName string) *StandardErrReply {
	return MakeErrReply(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
}

type SimpleStringReply struct {
	Status string // 状态字符串
}

func MakeSimpleStringReply(status string) *SimpleStringReply {
	return &SimpleStringReply{
		Status: status,
	}
}

func (r *SimpleStringReply) ToBytes() []byte {
	return []byte("+" + r.Status + "\r\n")
}

type IntReply struct {
	IntVal int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		IntVal: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.IntVal, 10) + string(CRLF))
}

type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + string(CRLF))
}

// 辅助：检查是否是 Error 类型
func IsErrorReply(reply Reply) bool {
	return reply.ToBytes()[0] == '-'
}

type BulkReply struct {
	Arg []byte // 实际数据
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return []byte("$-1\r\n") // Null Bulk String
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + string(CRLF) + string(r.Arg) + string(CRLF))
}

// 预定义 Null 回复
var NullBulkReply = &BulkReply{Arg: nil}

func MakeNullBulkReply() *BulkReply {
	return NullBulkReply
}

type MultiBulkReply struct {
	Args [][]byte // 二维字节切片，最通用的形式
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buf []byte
	argLen := len(r.Args)

	// 写入 Header: *2\r\n
	buf = append(buf, "*"...)
	buf = append(buf, []byte(strconv.Itoa(argLen))...)
	buf = append(buf, CRLF...)

	// 循环写入每个元素
	for _, arg := range r.Args {
		if arg == nil {
			buf = append(buf, []byte("$-1\r\n")...)
		} else {
			// $3\r\n
			buf = append(buf, "$"...)
			buf = append(buf, []byte(strconv.Itoa(len(arg)))...)
			buf = append(buf, CRLF...)
			// val\r\n
			buf = append(buf, arg...)
			buf = append(buf, CRLF...)
		}
	}
	return buf
}
