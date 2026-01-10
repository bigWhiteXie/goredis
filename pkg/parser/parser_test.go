package parser

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// TestParser_RESP_AllInOne 在一个 Test 函数里覆盖所有场景，用 t.Run 拆成独立用例
func TestParser_RESP_AllInOne(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  interface{}
		err   error
	}{
		// Simple String
		{name: "SimpleString", input: "+OK\r\n", want: "OK"},

		// Error
		{name: "Error", input: "-ERR unknown command\r\n", want: RespError{Message: "ERR unknown command"}},

		// Integer
		{name: "Integer", input: ":42\r\n", want: int64(42)},

		// Bulk String
		{name: "BulkString", input: "$6\r\nfoobar\r\n", want: []byte("foobar")},
		{name: "NullBulkString", input: "$-1\r\n", want: nil},

		// Array
		{name: "Array", input: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", want: []interface{}{[]byte("foo"), []byte("bar")}},
		{name: "NullArray", input: "*-1\r\n", want: nil},

		// 协议错误
		{name: "UnknownType", input: "?what\r\n", err: errors.New("protocol error: unknown RESP type")},
		{name: "InvalidBulkLen", input: "$abc\r\n", err: errors.New("protocol error: invalid bulk length")},
		{name: "InvalidArrayLen", input: "*xyz\r\n", err: errors.New("protocol error: invalid array length")},
		{name: "MissingCRLF", input: "+OK\n", err: errors.New("protocol error: invalid line ending"), want: []byte(nil)},
	}

	for _, tc := range tests {
		tc := tc // 捕获循环变量
		t.Run(tc.name, func(t *testing.T) {
			p := NewParser(bytes.NewBufferString(tc.input))
			got, err := p.Parse()

			// 先判断错误是否符合预期
			if tc.err != nil {
				if err == nil || err.Error() != tc.err.Error() {
					t.Fatalf("expected error %q, got %q", tc.err, err)
				}
				return // 错误场景已验证完
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 再判断值是否 DeepEqual
			if tc.want != nil {
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("expected %#v, got %#v", tc.want, got)
				}
			}

		})
	}
}
