package common

import (
	"bytes"
	"log"
	"os"
	"testing"
)

func TestCommon_All(t *testing.T) {
	t.Run("ToCmdLine", func(t *testing.T) {
		tests := []struct {
			name  string
			input interface{}
			want  [][]byte
			ok    bool
		}{
			{"empty_arr", []interface{}{}, [][]byte{}, true},
			{"normal_arr", []interface{}{[]byte("SET"), []byte("k"), []byte("v")}, [][]byte{[]byte("SET"), []byte("k"), []byte("v")}, true},
			{"arr_elem_not_bytes", []interface{}{"not_bytes"}, nil, false},
			{"empty_str", "", [][]byte{[]byte("")}, true},
			{"normal_str", "GET a", [][]byte{[]byte("GET"), []byte("a")}, true},
			{"unknown_type", 123, nil, false},
		}
		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				got, ok := ToCmdLine(tc.input)
				if ok != tc.ok || !bytesSliceEqual(got, tc.want) {
					t.Errorf("ToCmdLine() = (%v, %v), want (%v, %v)", got, ok, tc.want, tc.ok)
				}
			})
		}
	})

	t.Run("LogBytesArr", func(t *testing.T) {
		// 确保不 panic 即可
		var buf bytes.Buffer
		log.SetOutput(&buf)
		defer log.SetOutput(os.Stderr)

		LogBytesArr("TEST", [][]byte{[]byte("a"), []byte("b")})
		if !bytes.Contains(buf.Bytes(), []byte("[TEST]")) {
			t.Error("log output missing prefix")
		}
	})

	t.Run("Abs", func(t *testing.T) {
		tests := []struct {
			n, want int
		}{
			{0, 0},
			{-5, 5},
			{7, 7},
		}
		for _, tc := range tests {
			if got := Abs(tc.n); got != tc.want {
				t.Errorf("Abs(%d) = %d, want %d", tc.n, got, tc.want)
			}
		}
	})

	t.Run("ParseInt", func(t *testing.T) {
		tests := []struct {
			input []byte
			want  int64
			ok    bool
		}{
			{[]byte("0"), 0, true},
			{[]byte("-123"), -123, true},
			{[]byte("9223372036854775807"), 9223372036854775807, true},
			{[]byte("abc"), 0, false},
			{[]byte(""), 0, false},
		}
		for _, tc := range tests {
			got, ok := ParseInt(tc.input)
			if got != tc.want || ok != tc.ok {
				t.Errorf("ParseInt(%q) = (%d, %v), want (%d, %v)", tc.input, got, ok, tc.want, tc.ok)
			}
		}
	})

	t.Run("CloneBytes", func(t *testing.T) {
		src := []byte("hello")
		cp := CloneBytes(src)
		if !bytes.Equal(cp, src) {
			t.Errorf("CloneBytes() = %q, want %q", cp, src)
		}
		// 确保深拷贝
		src[0] = 'H'
		if cp[0] == 'H' {
			t.Error("CloneBytes should be deep copy")
		}
	})
}

// ---------------- 辅助 ----------------
func bytesSliceEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
