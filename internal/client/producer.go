package client

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Producer struct {
	addr   string
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Producer{
		addr:   addr,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

func (p *Producer) Close() error {
	return p.conn.Close()
}

func (p *Producer) Send(topic string, partition int, key, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cmd := fmt.Sprintf(
		"PRODUCE %s %d %s %s\n",
		topic,
		partition,
		string(key),
		string(value),
	)

	if _, err := p.writer.WriteString(cmd); err != nil {
		return -1, err
	}
	if err := p.writer.Flush(); err != nil {
		return -1, err
	}

	line, err := p.reader.ReadString('\n')
	if err != nil {
		return -1, err
	}

	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "ERR ") {
		return -1, fmt.Errorf(strings.TrimPrefix(line, "ERR "))
	}

	parts := strings.Split(line, " ")
	if len(parts) != 2 || parts[0] != "OK" {
		return -1, fmt.Errorf("unexpected response: %s", line)
	}

	offset, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return -1, err
	}

	return offset, nil
}
