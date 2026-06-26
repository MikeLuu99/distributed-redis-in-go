package main

import (
	"net/http"
	"testing"
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
