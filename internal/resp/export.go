package resp

import "fmt"

var OkReply = &SimpleStringReply{Status: "OK"}

func MakeOkReply() *SimpleStringReply {
	return OkReply
}

func MakeArgNumErrReply(cmdName string) *StandardErrReply {
	return MakeErrReply(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmdName))
}
