package data

import (
	"reflect"
	"testing"
)

func TestNewStringFromBytes(t *testing.T) {
	t.Run("integer string", func(t *testing.T) {
		s := NewStringFromBytes([]byte("123"))
		if !s.isInt || s.valInt != 123 {
			t.Errorf("expected int 123, got isInt=%v, valInt=%d", s.isInt, s.valInt)
		}
	})

	t.Run("negative integer", func(t *testing.T) {
		s := NewStringFromBytes([]byte("-456"))
		if !s.isInt || s.valInt != -456 {
			t.Errorf("expected int -456")
		}
	})

	t.Run("non-integer string", func(t *testing.T) {
		s := NewStringFromBytes([]byte("hello"))
		if s.isInt || string(s.valRaw) != "hello" {
			t.Errorf("expected raw 'hello', got isInt=%v, valRaw=%q", s.isInt, s.valRaw)
		}
	})

	t.Run("float string (should be raw)", func(t *testing.T) {
		s := NewStringFromBytes([]byte("3.14"))
		if s.isInt || string(s.valRaw) != "3.14" {
			t.Errorf("float should be treated as raw string")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		s := NewStringFromBytes([]byte(""))
		if s.isInt || len(s.valRaw) != 0 {
			t.Errorf("empty string should be raw")
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("int value", func(t *testing.T) {
		s := &SimpleString{isInt: true, valInt: 42}
		got := s.Get()
		if string(got) != "42" {
			t.Errorf("expected '42', got %q", got)
		}
	})

	t.Run("raw value", func(t *testing.T) {
		s := &SimpleString{isInt: false, valRaw: []byte("world")}
		got := s.Get()
		if string(got) != "world" {
			t.Errorf("expected 'world', got %q", got)
		}
	})
}

func TestSet(t *testing.T) {
	s := &SimpleString{isInt: true, valInt: 100}

	t.Run("set to new integer", func(t *testing.T) {
		s.Set([]byte("200"))
		if !s.isInt || s.valInt != 200 {
			t.Errorf("expected int 200 after set")
		}
	})

	t.Run("set to non-integer", func(t *testing.T) {
		s.Set([]byte("hello"))
		if s.isInt || string(s.valRaw) != "hello" {
			t.Errorf("expected raw 'hello'")
		}
	})

	t.Run("set back to integer", func(t *testing.T) {
		s.Set([]byte("-50"))
		if !s.isInt || s.valInt != -50 {
			t.Errorf("expected int -50")
		}
	})
}

func TestIncrBy(t *testing.T) {
	t.Run("increment int", func(t *testing.T) {
		s := &SimpleString{isInt: true, valInt: 10}
		result, err := s.IncrBy(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != 15 || s.valInt != 15 {
			t.Errorf("expected 15, got %d", result)
		}
	})

	t.Run("decrement int", func(t *testing.T) {
		s := &SimpleString{isInt: true, valInt: 10}
		result, err := s.IncrBy(-3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != 7 {
			t.Errorf("expected 7, got %d", result)
		}
	})

	t.Run("error on non-int", func(t *testing.T) {
		s := &SimpleString{isInt: false, valRaw: []byte("abc")}
		_, err := s.IncrBy(1)
		if err == nil {
			t.Error("expected error for non-integer")
		}
		if err.Error() != "value is not an integer" {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestClone(t *testing.T) {
	t.Run("clone int", func(t *testing.T) {
		orig := &SimpleString{isInt: true, valInt: 99}
		clone := orig.Clone().(*SimpleString)

		if !clone.isInt || clone.valInt != 99 {
			t.Error("cloned int mismatch")
		}
		if orig == clone {
			t.Error("clone should be a new instance")
		}
	})

	t.Run("clone raw", func(t *testing.T) {
		orig := &SimpleString{isInt: false, valRaw: []byte("test")}
		clone := orig.Clone().(*SimpleString)

		if clone.isInt || string(clone.valRaw) != "test" {
			t.Error("cloned raw mismatch")
		}
		if &orig.valRaw[0] == &clone.valRaw[0] {
			t.Error("valRaw should be deep copied")
		}
	})
}

func TestToWriteCmdLine(t *testing.T) {
	t.Run("raw string", func(t *testing.T) {
		s := &SimpleString{isInt: false, valRaw: []byte("value")}
		cmd := s.ToWriteCmdLine("mykey")
		expected := [][]byte{[]byte("set"), []byte("mykey"), []byte("value")}
		if !reflect.DeepEqual(cmd, expected) {
			t.Errorf("expected %v, got %v", expected, cmd)
		}
	})

	t.Run("int string (should use Get())", func(t *testing.T) {
		s := &SimpleString{isInt: true, valInt: 123}
		cmd := s.ToWriteCmdLine("k")
		// 注意：当前实现直接用 valRaw（但 int 时 valRaw=nil），这是 BUG！
		// 正确做法应使用 s.Get()
		if len(cmd) != 3 || string(cmd[2]) != "123" {
			t.Errorf("ToWriteCmdLine failed for int: %v", cmd)
		}
	})
}
