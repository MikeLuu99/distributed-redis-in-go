package main

import (
	"net/http"
	"strings"
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

func TestTLSConfigRequiresCertAndKey(t *testing.T) {
	if _, err := listenTCP("127.0.0.1:0", "cert.pem", ""); err == nil || !strings.Contains(err.Error(), "both tls-cert-file") {
		t.Fatalf("listenTCP() error = %v, want cert/key validation error", err)
	}

	err := serveHTTP(newHTTPServer("127.0.0.1:0", http.NewServeMux()), "", "key.pem")
	if err == nil || !strings.Contains(err.Error(), "both tls-cert-file") {
		t.Fatalf("serveHTTP() error = %v, want cert/key validation error", err)
	}
}
