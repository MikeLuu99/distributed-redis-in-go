package resp

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type chunkedReader struct {
	chunks []string
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := r.chunks[0]
	r.chunks = r.chunks[1:]
	return copy(p, chunk), nil
}

func TestParserHandlesPartialReads(t *testing.T) {
	parser := NewParser(&chunkedReader{
		chunks: []string{"*2\r\n$4\r\n", "PING\r\n$4\r\n", "PONG\r\n"},
	})

	value, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	assertBulkArray(t, value, "PING", "PONG")
}

func TestParserHandlesPipelinedCommands(t *testing.T) {
	parser := NewParser(strings.NewReader("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))

	first, err := parser.Parse()
	if err != nil {
		t.Fatalf("first Parse() error = %v", err)
	}
	assertBulkArray(t, first, "PING")

	second, err := parser.Parse()
	if err != nil {
		t.Fatalf("second Parse() error = %v", err)
	}
	assertBulkArray(t, second, "GET", "key")
}

func TestParserRejectsMalformedBulkStringTerminator(t *testing.T) {
	parser := NewParser(strings.NewReader("$3\r\nabcxx"))

	_, err := parser.Parse()
	if err == nil || !strings.Contains(err.Error(), "missing CRLF") {
		t.Fatalf("Parse() error = %v, want missing CRLF", err)
	}
}

func TestParserRejectsOversizedBulkString(t *testing.T) {
	parser := NewParser(strings.NewReader("$536870913\r\n"))

	_, err := parser.Parse()
	if err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("Parse() error = %v, want exceeds max", err)
	}
}

func TestParserRejectsOversizedArray(t *testing.T) {
	parser := NewParser(strings.NewReader("*1025\r\n"))

	_, err := parser.Parse()
	if err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("Parse() error = %v, want exceeds max", err)
	}
}

func TestParserRejectsNullBulkString(t *testing.T) {
	parser := NewParser(strings.NewReader("$-1\r\n"))

	_, err := parser.Parse()
	if !errors.Is(err, ErrNullBulkString) {
		t.Fatalf("Parse() error = %v, want ErrNullBulkString", err)
	}
}

func assertBulkArray(t *testing.T, value RESPValue, want ...string) {
	t.Helper()

	if value.Type != RESPArray {
		t.Fatalf("value.Type = %v, want RESPArray", value.Type)
	}
	if len(value.Array) != len(want) {
		t.Fatalf("len(value.Array) = %d, want %d", len(value.Array), len(want))
	}
	for i := range want {
		if value.Array[i].Type != RESPBulkString || value.Array[i].String != want[i] {
			t.Fatalf("value.Array[%d] = %+v, want bulk string %q", i, value.Array[i], want[i])
		}
	}
}
