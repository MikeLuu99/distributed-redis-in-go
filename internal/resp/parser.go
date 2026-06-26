package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulkStringSize = 512 * 1024 * 1024
	MaxArrayElements  = 1024
	MaxLineSize       = 64 * 1024
)

var ErrNullBulkString = errors.New("null bulk string is not supported")

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

type Parser struct {
	reader *bufio.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{reader: bufio.NewReader(r)}
}

func (p *Parser) Parse() (RESPValue, error) {
	prefix, err := p.reader.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	switch prefix {
	case '+':
		value, err := p.readLine()
		return RESPValue{Type: RESPString, String: value}, err
	case '-':
		value, err := p.readLine()
		return RESPValue{Type: RESPError, String: value}, err
	case ':':
		value, err := p.readInteger()
		return RESPValue{Type: RESPInteger, Int: value}, err
	case '$':
		value, err := p.readBulkString()
		return RESPValue{Type: RESPBulkString, String: value}, err
	case '*':
		return p.readArray()
	default:
		return RESPValue{}, fmt.Errorf("invalid RESP type byte %q", prefix)
	}
}

func (p *Parser) readLine() (string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > MaxLineSize {
		return "", fmt.Errorf("RESP line exceeds %d bytes", MaxLineSize)
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("RESP line missing CRLF terminator")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (p *Parser) readInteger() (int64, error) {
	line, err := p.readLine()
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid RESP integer %q: %w", line, err)
	}
	return value, nil
}

func (p *Parser) readLength(max int64) (int64, error) {
	length, err := p.readInteger()
	if err != nil {
		return 0, err
	}
	if length < -1 {
		return 0, fmt.Errorf("invalid RESP length %d", length)
	}
	if length > max {
		return 0, fmt.Errorf("RESP length %d exceeds max %d", length, max)
	}
	return length, nil
}

func (p *Parser) readBulkString() (string, error) {
	length, err := p.readLength(MaxBulkStringSize)
	if err != nil {
		return "", err
	}
	if length == -1 {
		return "", ErrNullBulkString
	}

	data := make([]byte, length+2)
	if _, err := io.ReadFull(p.reader, data); err != nil {
		return "", err
	}
	if data[length] != '\r' || data[length+1] != '\n' {
		return "", errors.New("bulk string missing CRLF terminator")
	}

	return string(data[:length]), nil
}

func (p *Parser) readArray() (RESPValue, error) {
	count, err := p.readLength(MaxArrayElements)
	if err != nil {
		return RESPValue{}, err
	}
	if count == -1 {
		return RESPValue{}, errors.New("null array is not supported")
	}

	values := make([]RESPValue, count)
	for i := range values {
		value, err := p.Parse()
		if err != nil {
			return RESPValue{}, err
		}
		values[i] = value
	}

	return RESPValue{Type: RESPArray, Array: values}, nil
}
