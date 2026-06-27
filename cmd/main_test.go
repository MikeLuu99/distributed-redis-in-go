package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/web"
)

func TestNewHTTPServerSetsTimeouts(t *testing.T) {
	server := newHTTPServer("127.0.0.1:0", http.NewServeMux())

	if server.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", server.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if server.ReadTimeout != httpReadTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", server.ReadTimeout, httpReadTimeout)
	}
	if server.WriteTimeout != httpWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", server.WriteTimeout, httpWriteTimeout)
	}
	if server.IdleTimeout != httpIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", server.IdleTimeout, httpIdleTimeout)
	}
}

func TestHTTPMuxRegistersDebugVars(t *testing.T) {
	database, closeDB, err := db.NewDatabase(t.TempDir()+"/test.db", false)
	if err != nil {
		t.Fatalf("NewDatabase() error = %v", err)
	}
	defer closeDB()

	srv := web.NewServer(database, &config.Shards{
		Count:  1,
		CurIdx: 0,
		Addrs:  map[int]string{0: "127.0.0.1:8080"},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	newHTTPMux(srv).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/vars status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestTLSConfigRequiresCertAndKey(t *testing.T) {
	if _, err := listenTCP("127.0.0.1:0", "cert.pem", ""); err == nil || !strings.Contains(err.Error(), "both tls-cert-file") {
		t.Fatalf("listenTCP() error = %v, want cert/key validation error", err)
	}

	err := serveHTTP(newHTTPServer("127.0.0.1:0", http.NewServeMux()), "", "key.pem")
	if err == nil || !strings.Contains(err.Error(), "both tls-cert-file") {
		t.Fatalf("serveHTTP() error = %v, want cert/key validation error", err)
	}
}

func TestRESPServerRejectsConnectionsOverLimit(t *testing.T) {
	listener := newTestListener()
	defer listener.Close()

	release := make(chan struct{})
	resp := newRESPServer(listener, 1, func(conn net.Conn) {
		<-release
		conn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- resp.Serve(ctx)
	}()

	serverConn1, clientConn1 := net.Pipe()
	defer clientConn1.Close()
	listener.accept(serverConn1)

	serverConn2, clientConn2 := net.Pipe()
	defer clientConn2.Close()
	listener.accept(serverConn2)

	buf := make([]byte, len("-ERR max connections reached\r\n"))
	if _, err := io.ReadFull(clientConn2, buf); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	if got, want := string(buf), "-ERR max connections reached\r\n"; got != want {
		t.Fatalf("overflow response = %q, want %q", got, want)
	}

	close(release)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestRESPServerClosesActiveConnectionsOnShutdown(t *testing.T) {
	listener := newTestListener()
	defer listener.Close()

	resp := newRESPServer(listener, 1, func(conn net.Conn) {
		io.Copy(io.Discard, conn)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- resp.Serve(ctx)
	}()

	serverConn, clientConn := net.Pipe()
	listener.accept(serverConn)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	if _, err := clientConn.Write([]byte("ping")); err == nil {
		t.Fatal("expected client connection to be closed after shutdown")
	}
	clientConn.Close()
}

type testListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newTestListener() *testListener {
	return &testListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *testListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *testListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

func (l *testListener) Addr() net.Addr {
	return dummyAddr("test")
}

func (l *testListener) accept(conn net.Conn) {
	select {
	case l.conns <- conn:
	case <-time.After(time.Second):
		panic("listener did not accept connection")
	}
}

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }
