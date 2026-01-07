package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

type Parser struct {
	r *bufio.Reader
}

func NewParser(reader io.Reader) *Parser {
	return &Parser{
		r: bufio.NewReader(reader),
	}
}

func (p *Parser) Parse() (interface{}, error) {
	b, err := p.r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case '+':
		return p.parseSimpleString()
	case '-':
		return p.parseError()
	case ':':
		return p.parseInteger()
	case '$':
		return p.parseBulkString()
	case '*':
		return p.parseArray()
	default:
		return nil, errors.New("protocol error: unknown RESP type")
	}
}

func (p *Parser) parseSimpleString() (string, error) {
	line, err := p.readLine()
	if err != nil {
		return "", err
	}
	return line, nil
}

type RespError struct {
	Message string
}

func (e RespError) Error() string {
	return e.Message
}

func (p *Parser) parseError() (RespError, error) {
	line, err := p.readLine()
	if err != nil {
		return RespError{}, err
	}
	return RespError{Message: line}, nil
}

func (p *Parser) parseInteger() (int64, error) {
	line, err := p.readLine()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(line, 10, 64)
}

func (p *Parser) parseBulkString() ([]byte, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, errors.New("protocol error: invalid bulk length")
	}

	// NULL bulk string
	if length == -1 {
		return nil, nil
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(p.r, buf)
	if err != nil {
		return nil, err
	}

	// consume \r\n
	if err := p.expectCRLF(); err != nil {
		return nil, err
	}

	return buf, nil
}

func (p *Parser) parseArray() ([]interface{}, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	n, err := strconv.Atoi(line)
	if err != nil {
		return nil, errors.New("protocol error: invalid array length")
	}

	// NULL array
	if n == -1 {
		return nil, nil
	}

	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		elem, err := p.Parse()
		if err != nil {
			return nil, err
		}
		result[i] = elem
	}
	return result, nil
}

func (p *Parser) readLine() (string, error) {
	line, err := p.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("protocol error: invalid line ending")
	}
	return line[:len(line)-2], nil
}

func (p *Parser) expectCRLF() error {
	if b, err := p.r.ReadByte(); err != nil || b != '\r' {
		return errors.New("protocol error: expected CR")
	}
	if b, err := p.r.ReadByte(); err != nil || b != '\n' {
		return errors.New("protocol error: expected LF")
	}
	return nil
}
