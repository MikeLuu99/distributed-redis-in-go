package resp

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
)

type RESPValue struct {
	Type   RESPType
	String string
	Int    int64
	Array  []RESPValue
}

type RESPType int

const (
	RESPString RESPType = iota
	RESPError
	RESPInteger
	RESPBulkString
	RESPArray
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

func readArray(c io.ReadWriter, buf *bytes.Buffer, rp *RESPParser) (RESPValue, error) {
	count, err := readLength(buf)

	if err != nil {
		return RESPValue{}, err
	}

	log.Println("array length: ", count)
	var elems []RESPValue = make([]RESPValue, count)
	for i := range elems {
		elem, err := rp.parseSingle()
		if err != nil {
			return RESPValue{}, err
		}
		elems[i] = elem
	}

	return RESPValue{Type: RESPArray, Array: elems}, nil
}

func (rp *RESPParser) parseSingle() (RESPValue, error) {
	b, err := rp.buf.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}
	switch b {
	case '+':
		val, err := readSimpleString(rp.c, rp.buf)
		return RESPValue{Type: RESPString, String: val}, err
	case '-':
		val, err := readError(rp.c, rp.buf)
		return RESPValue{Type: RESPError, String: val}, err
	case ':':
		val, err := readInteger(rp.c, rp.buf)
		return RESPValue{Type: RESPInteger, Int: val}, err
	case '$':
		val, err := readBulkString(rp.c, rp.buf)
		return RESPValue{Type: RESPBulkString, String: val}, err
	case '*':
		return readArray(rp.c, rp.buf, rp)
	}
	return RESPValue{}, errors.New("invalid input")
}

func ParseRESP(c net.Conn, input_buf []byte) (RESPValue, error) {
	var b []byte
	var buf *bytes.Buffer = bytes.NewBuffer(b)
	buf.Write(input_buf)

	rp := &RESPParser{
		c:   c,
		buf: buf,
	}

	return rp.parseSingle()
}
