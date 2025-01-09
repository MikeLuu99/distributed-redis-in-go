package main

import (
	"bytes"
	"net"
)

func ParseRESP(c net.Conn, input_buf []byte) {
	var b []byte
	var buf *bytes.Buffer = bytes.NewBuffer(b)
	buf.Write(input_buf)
}
