package tdx

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHandshakeFailServer 接受 TCP 连接后立刻关闭，不发任何握手响应，
// 用来复现休眠唤醒/网络抖动时「DialTimeout 成功但 readResponse 读失败」的场景。
func fakeHandshakeFailServer(t *testing.T, acceptN int) (host string, port int, done <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	ch := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(ch)
		defer ln.Close()
		for i := 0; i < acceptN; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
		// 等一小会儿，让客户端的读失败先发生
		time.Sleep(200 * time.Millisecond)
		once.Do(func() {})
	}()
	return addr.IP.String(), addr.Port, ch
}

// 修复前：Connect() 失败分支对已置 nil 的 c.conn 调用 Close() 会 panic。
func TestConnectHandshakeFailureDoesNotPanic(t *testing.T) {
	host, port, done := fakeHandshakeFailServer(t, 1)
	c := NewClient(host, port)
	c.timeout = 800 * time.Millisecond

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Connect() 仍然 panic: %v", r)
			}
		}()
		err = c.Connect()
	}()
	<-done

	if err == nil {
		t.Fatalf("握手失败应返回 error")
	}
	if !strings.Contains(err.Error(), "握手失败") {
		t.Fatalf("错误信息不符合预期: %v", err)
	}
	if c.IsConnected() {
		t.Fatalf("握手失败后 conn 应为 nil")
	}
	t.Logf("OK: 返回 error 且无 panic -> %v", err)
}

// 测速协程即使遇到意外 panic 也必须被降级为失败结果，绝不逃逸杀进程。
func TestBenchmarkIsolatesPanic(t *testing.T) {
	host, port, done := fakeHandshakeFailServer(t, 1)
	t.Setenv("TDX_HQ_SERVERS", fmt.Sprintf("%s:%d", host, port))

	results := Benchmark(800 * time.Millisecond)
	<-done

	if len(results) != 1 {
		t.Fatalf("应返回 1 条测速结果, got %d", len(results))
	}
	if results[0].OK {
		t.Fatalf("握手失败不应标记为 OK")
	}
	t.Logf("OK: 测速失败被正常记录 -> %v", results[0].Error)
}
