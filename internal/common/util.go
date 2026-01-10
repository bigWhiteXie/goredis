package common

import (
	"log"
	"strconv"
	"strings"
)

func ToCmdLine(payload interface{}) ([][]byte, bool) {
	switch payload.(type) {
	case []interface{}:
		arr, _ := payload.([]interface{})
		cmd := make([][]byte, len(arr))
		for i, v := range arr {
			bs, ok := v.([]byte)
			if !ok {
				return nil, false
			}
			cmd[i] = bs
		}

		return cmd, true
	case string:
		str := payload.(string)
		arr := strings.Split(str, " ")
		cmd := make([][]byte, len(arr))
		for i, v := range arr {
			cmd[i] = []byte(v)

		}
		return cmd, true
	}

	return nil, false
}

func LogBytesArr(prefix string, content [][]byte) {
	sb := strings.Builder{}
	sb.WriteString("[" + prefix + "] ")
	for _, v := range content {
		sb.WriteString(string(v) + " ")
	}

	log.Println(sb.String())
}

func Abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func ParseInt(b []byte) (int64, bool) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	return v, err == nil
}

func CloneBytes(b []byte) []byte {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
