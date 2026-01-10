package persistant

import (
	"testing"
)

func TestReplBacklog(t *testing.T) {
	t.Run("initial state", func(t *testing.T) {
		rb := NewReplBacklog(10, 100)
		if rb.GetStartOffset() != 100 {
			t.Errorf("expected start=100, got %d", rb.GetStartOffset())
		}
		if rb.end != 100 {
			t.Errorf("expected end=100, got %d", rb.end)
		}
		if rb.CanServe(100) {
			t.Error("should serve offset 100 (empty buffer)")
		}
		if rb.CanServe(101) {
			t.Error("should not serve offset 101")
		}
	})

	t.Run("append small data", func(t *testing.T) {
		rb := NewReplBacklog(10, 0)
		data := []byte("hello")
		rb.Append(data)

		if rb.end != 5 {
			t.Errorf("end should be 5, got %d", rb.end)
		}
		if rb.GetStartOffset() != 0 {
			t.Errorf("start should remain 0, got %d", rb.GetStartOffset())
		}

		// Read full
		read := rb.ReadFrom(0)
		if string(read) != "hello" {
			t.Errorf("expected 'hello', got %q", read)
		}

		// Read partial
		read = rb.ReadFrom(2)
		if string(read) != "llo" {
			t.Errorf("expected 'llo', got %q", read)
		}

		// CanServe
		if !rb.CanServe(0) || !rb.CanServe(4) || rb.CanServe(5) {
			t.Error("CanServe logic error")
		}
	})

	t.Run("append until full", func(t *testing.T) {
		rb := NewReplBacklog(5, 0)
		rb.Append([]byte("12345")) // exactly fill

		if rb.end != 5 {
			t.Errorf("end should be 5")
		}
		if rb.GetStartOffset() != 0 {
			t.Errorf("start should be 0")
		}

		read := rb.ReadFrom(0)
		if string(read) != "12345" {
			t.Errorf("full read failed: %q", read)
		}
	})

	t.Run("circular overwrite", func(t *testing.T) {
		rb := NewReplBacklog(5, 0)
		rb.Append([]byte("12345")) // [1,2,3,4,5] start=0, end=5
		rb.Append([]byte("6"))     // overwrite first byte → [6,2,3,4,5], start=1, end=6

		if rb.GetStartOffset() != 1 {
			t.Errorf("start should be 1 after overwrite, got %d", rb.GetStartOffset())
		}
		if rb.end != 6 {
			t.Errorf("end should be 6, got %d", rb.end)
		}

		// Should not serve offset 0 (overwritten)
		if rb.CanServe(0) {
			t.Error("offset 0 should not be servable")
		}
		if !rb.CanServe(1) || !rb.CanServe(5) {
			t.Error("offsets 1-5 should be servable")
		}

		// Read from offset 1
		read := rb.ReadFrom(1)
		if string(read) != "23456" {
			t.Errorf("expected '23456', got %q", read)
		}
	})

	t.Run("multiple overwrites", func(t *testing.T) {
		rb := NewReplBacklog(3, 10)
		rb.Append([]byte("ABC")) // start=10, end=13
		rb.Append([]byte("D"))   // start=11, end=14 → buf=[D,B,C]
		rb.Append([]byte("E"))   // start=12, end=15 → buf=[D,E,C]
		rb.Append([]byte("F"))   // start=13, end=16 → buf=[D,E,F]

		if rb.GetStartOffset() != 13 {
			t.Errorf("start should be 13, got %d", rb.GetStartOffset())
		}

		read := rb.ReadFrom(13)
		if string(read) != "DEF" {
			t.Errorf("expected 'DEF', got %q", read)
		}

		// Cannot read before start
		if rb.ReadFrom(12) != nil {
			t.Error("should not read offset 12")
		}
	})

	t.Run("read from middle after overwrite", func(t *testing.T) {
		rb := NewReplBacklog(4, 0)
		rb.Append([]byte("1234")) // [1,2,3,4] start=0, end=4
		rb.Append([]byte("56"))   // [5,6,3,4] start=2, end=6

		// Read from offset 3 (which is '4')
		read := rb.ReadFrom(3)
		if string(read) != "456" {
			t.Errorf("expected '456', got %q", read)
		}

		// Verify buffer layout:
		// global offset: 2->'3', 3->'4', 4->'5', 5->'6'
		// buf[0]='5' (offset 4), buf[1]='6' (offset 5), buf[2]='3' (offset 2), buf[3]='4' (offset 3)
	})

	t.Run("empty read", func(t *testing.T) {
		rb := NewReplBacklog(10, 0)
		if rb.ReadFrom(0) != nil {
			t.Error("read from empty backlog should return nil")
		}
	})

	t.Run("invalid offset read", func(t *testing.T) {
		rb := NewReplBacklog(5, 100)
		rb.Append([]byte("data"))

		// Before start
		if rb.ReadFrom(99) != nil {
			t.Error("should not read before start")
		}
		// At end
		if rb.ReadFrom(104) != nil {
			t.Error("should not read at end")
		}
		// After end
		if rb.ReadFrom(105) != nil {
			t.Error("should not read after end")
		}
	})
}
