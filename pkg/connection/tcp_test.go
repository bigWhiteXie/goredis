package connection

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// 辅助：返回一对内存 pipe，左边给 TCPConnection，右边给测试代码
func newPipeConns() (srv, cli net.Conn) {
	srv, cli = net.Pipe()
	return
}

// TestTCPConnection_All 在一个 Test 里跑完所有场景
func TestTCPConnection_All(t *testing.T) {
	t.Run("FieldsAfterNew", func(t *testing.T) {
		srv, _ := newPipeConns()
		defer srv.Close()
		c := NewTCPConnection(srv).(*TCPConnection)

		if c.GetDBIndex() != 0 {
			t.Errorf("default dbIndex want 0, got %d", c.GetDBIndex())
		}
		if c.IsSlave() {
			t.Error("new conn should not be slave")
		}
		if c.IsClosed() {
			t.Error("new conn should not be closed")
		}
	})

	t.Run("SelectDB", func(t *testing.T) {
		srv, _ := newPipeConns()
		defer srv.Close()
		c := NewTCPConnection(srv).(*TCPConnection)

		c.SelectDB(3)
		if got := c.GetDBIndex(); got != 3 {
			t.Errorf("after SelectDB(3) want 3, got %d", got)
		}
	})

	t.Run("SetSlave/IsSlave", func(t *testing.T) {
		srv, _ := newPipeConns()
		defer srv.Close()
		c := NewTCPConnection(srv).(*TCPConnection)

		c.SetSlave()
		if !c.IsSlave() {
			t.Error("expected slave role")
		}
	})

	t.Run("Write", func(t *testing.T) {
		srv, cli := newPipeConns()
		defer srv.Close()
		defer cli.Close()
		c := NewTCPConnection(srv).(*TCPConnection)

		msg := []byte("PING\r\n")
		// 先启动读端
		done := make(chan []byte, 1)
		go func() {
			buf := make([]byte, 64)
			n, _ := cli.Read(buf)
			done <- buf[:n]
		}()

		// 再写，一定不会阻塞
		n, err := c.Write(msg)
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		if n != len(msg) {
			t.Errorf("write length want %d, got %d", len(msg), n)
		}

		// 等读端结果
		received := <-done
		if !bytes.Equal(received, msg) {
			t.Errorf("received %q, want %q", received, msg)
		}
	})

	t.Run("WriteAfterClose", func(t *testing.T) {
		srv, _ := newPipeConns()
		c := NewTCPConnection(srv).(*TCPConnection)
		c.Close()

		_, err := c.Write([]byte("x"))
		if err == nil || err.Error() != "connection closed" {
			t.Errorf("expected 'connection closed', got %v", err)
		}
	})

	t.Run("CloseIdempotent", func(t *testing.T) {
		srv, _ := newPipeConns()
		c := NewTCPConnection(srv).(*TCPConnection)

		if err := c.Close(); err != nil {
			t.Fatalf("first Close error: %v", err)
		}
		if !c.IsClosed() {
			t.Error("IsClosed should be true")
		}
		// 再关一次不应出错
		if err := c.Close(); err != nil {
			t.Errorf("second Close should be nil, got %v", err)
		}
	})

	t.Run("ConcurrentWriteAndClose", func(t *testing.T) {
		srv, _ := newPipeConns()
		c := NewTCPConnection(srv).(*TCPConnection)

		var wg sync.WaitGroup
		wg.Add(10)

		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				// 给底层 conn 加写超时，防止阻塞 30s
				_ = srv.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
				_, _ = c.Write([]byte("x")) // 忽略错误，只测竞态
			}()
		}

		time.Sleep(5 * time.Millisecond)
		_ = c.Close() // 关掉连接，让后续 Write 直接返回 "connection closed"
		wg.Wait()     // 现在 goroutine 都能在 50 ms 内返回，不会挂死
	})

	t.Run("RemoteAddr", func(t *testing.T) {
		srv, _ := newPipeConns()
		defer srv.Close()
		c := NewTCPConnection(srv)
		if c.RemoteAddr() == "" {
			t.Error("RemoteAddr should not be empty")
		}
	})
}

// 额外：如果想把 Connection 接口里所有方法都跑一遍，可以写一个“接口合规”测试
func TestTCPConnection_ImplementsConnectionInterface(t *testing.T) {
	// 编译期检查：*TCPConnection 实现了 Connection 接口
	var _ Connection = (*TCPConnection)(nil)
}
