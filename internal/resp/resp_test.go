package resp

import (
	"bytes"
	"testing"
)

func TestRESP_All(t *testing.T) {
	tests := []struct {
		name  string
		reply Reply
		want  []byte
	}{
		// SimpleString
		{name: "SimpleString_OK", reply: MakeOkReply(), want: []byte("+OK\r\n")},
		{name: "SimpleString_Custom", reply: MakeSimpleStringReply("PONG"), want: []byte("+PONG\r\n")},

		// Error
		{name: "Error_WrongArgs", reply: MakeArgNumErrReply("GET"), want: []byte("-ERR wrong number of arguments for 'GET' command\r\n")},
		{name: "Error_Custom", reply: MakeErrReply("ERR unknown command"), want: []byte("-ERR unknown command\r\n")},

		// Int
		{name: "Int_Zero", reply: MakeIntReply(0), want: []byte(":0\r\n")},
		{name: "Int_Negative", reply: MakeIntReply(-99), want: []byte(":-99\r\n")},
		{name: "Int_Positive", reply: MakeIntReply(1024), want: []byte(":1024\r\n")},

		// Bulk
		{name: "Bulk_Empty", reply: MakeBulkReply([]byte("")), want: []byte("$0\r\n\r\n")},
		{name: "Bulk_Hello", reply: MakeBulkReply([]byte("hello")), want: []byte("$5\r\nhello\r\n")},
		{name: "Bulk_Null", reply: MakeNullBulkReply(), want: []byte("$-1\r\n")},
		{name: "Bulk_NilBytes", reply: MakeBulkReply(nil), want: []byte("$-1\r\n")}, // 显式 nil

		// MultiBulk
		{name: "MultiBulk_Empty", reply: MakeMultiBulkReply([][]byte{}), want: []byte("*0\r\n")},
		{name: "MultiBulk_Mixed", reply: MakeMultiBulkReply([][]byte{
			[]byte("hello"),
			nil,
			[]byte("world"),
		}), want: []byte("*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n")},
		{name: "MultiBulk_AllNull", reply: MakeMultiBulkReply([][]byte{nil, nil}), want: []byte("*2\r\n$-1\r\n$-1\r\n")},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := tc.reply.ToBytes()
			if !bytes.Equal(got, tc.want) {
				t.Errorf("ToBytes() = %q, want %q", got, tc.want)
			}
		})
	}
}

// 单独测试 IsErrorReply 工具函数
func TestIsErrorReply(t *testing.T) {
	tests := []struct {
		name string
		r    Reply
		want bool
	}{
		{"simple_string", MakeSimpleStringReply("OK"), false},
		{"error_reply", MakeErrReply("ERR foo"), true},
		{"int_reply", MakeIntReply(1), false},
		{"bulk_reply", MakeBulkReply([]byte("x")), false},
		{"null_bulk", MakeNullBulkReply(), false},
		{"multi_bulk", MakeMultiBulkReply([][]byte{[]byte("a")}), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsErrorReply(tc.r); got != tc.want {
				t.Errorf("IsErrorReply() = %v, want %v", got, tc.want)
			}
		})
	}
}
