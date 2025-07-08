package resp

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
)

type RESPParser struct {
	c    io.ReadWriter
	buf  *bytes.Buffer
	tbuf []byte
}

func readStringUntilSr(buf *bytes.Buffer) (string, error) {
	s, err := buf.ReadString('\r')
	if err != nil {
		return "", err
	}

	buf.ReadByte()
	return s[:len(s)-1], nil
}

func readSimpleString(c io.ReadWriter, buf *bytes.Buffer) (string, error) {
	return readStringUntilSr(buf)
}

func readError(c io.ReadWriter, buf *bytes.Buffer) (string, error) {
	return readStringUntilSr(buf)
}

func readInteger(c io.ReadWriter, buf *bytes.Buffer) (int64, error) {
	s, err := readStringUntilSr(buf)
	if err != nil {
		return 0, err
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func readLength(buf *bytes.Buffer) (int64, error) {
	s, err := readStringUntilSr(buf)
	if err != nil {
		return 0, err
	}

	v, err := strconv.ParseInt(s, 10, 64)

	if err != nil {
		return 0, err
	}

	return v, nil

}

func readBulkString(c io.ReadWriter, buf *bytes.Buffer) (string, error) {
	len, err := readLength(buf)
	if err != nil {
		return "", err
	}

	bulkString := make([]byte, len)
	_, err = buf.Read(bulkString)
	if err != nil {
		return "", err
	}

	buf.ReadByte()
	buf.ReadByte()

	return string(bulkString), nil
}

func readArray(c io.ReadWriter, buf *bytes.Buffer, rp *RESPParser) (any, error) {
	count, err := readLength(buf)

	if err != nil {
		return nil, err
	}

	log.Println("array length: ", count)
	var elems []any = make([]any, count)
	for i := range elems {
		elem, err := rp.parseSingle()
		if err != nil {
			return nil, err
		}
		elems[i] = elem
	}

	return elems, nil
}

func (rp *RESPParser) parseSingle() (any, error) {
	b, err := rp.buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch b {
	case '+':
		return readSimpleString(rp.c, rp.buf)
	case '-':
		return readError(rp.c, rp.buf)
	case ':':
		return readInteger(rp.c, rp.buf)
	case '$':
		return readBulkString(rp.c, rp.buf)
	case '*':
		return readArray(rp.c, rp.buf, rp)
	}
	return nil, errors.New("invalid input")
}

func ParseRESP(c net.Conn, input_buf []byte) (any, error) {
	var b []byte
	var buf *bytes.Buffer = bytes.NewBuffer(b)
	buf.Write(input_buf)

	rp := &RESPParser{
		c:   c,
		buf: buf,
	}

	return rp.parseSingle()
}
