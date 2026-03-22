package client

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/EricNguyen1206/erionn-mq/internal/partition"
)

type Consumer struct {
	addr      string
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	topic     string
	partition int
	offset    int64
	mu        sync.Mutex
}

func NewConsumer(addr, topic string, partition int, offset int64) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		addr:      addr,
		conn:      conn,
		reader:    bufio.NewReader(conn),
		writer:    bufio.NewWriter(conn),
		topic:     topic,
		partition: partition,
		offset:    offset,
	}, nil
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}

func (c *Consumer) CurrentOffset() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

func (c *Consumer) SetOffset(offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset = offset
}

func (c *Consumer) Poll(max int) ([]partition.Record, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf(
		"FETCH %s %d %d %d\n",
		c.topic,
		c.partition,
		c.offset,
		max,
	)

	if _, err := c.writer.WriteString(cmd); err != nil {
		return nil, 0, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, 0, err
	}

	header, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}
	header = strings.TrimSpace(header)

	if strings.HasPrefix(header, "ERR ") {
		return nil, 0, fmt.Errorf(strings.TrimPrefix(header, "ERR "))
	}

	parts := strings.Split(header, " ")
	if len(parts) != 4 || parts[0] != "MESSAGES" || parts[2] != "HW" {
		return nil, 0, fmt.Errorf("unexpected fetch header: %s", header)
	}

	numMessages, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, 0, err
	}

	highWatermark, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, 0, err
	}

	records := make([]partition.Record, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		line = strings.TrimSpace(line)

		record, err := parseFetchRecordLine(line)
		if err != nil {
			return nil, 0, err
		}

		records = append(records, record)
	}

	endLine, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}
	endLine = strings.TrimSpace(endLine)
	if endLine != "END" {
		return nil, 0, fmt.Errorf("expected END, got %s", endLine)
	}

	if len(records) > 0 {
		c.offset = records[len(records)-1].Offset + 1
	}

	return records, highWatermark, nil
}

func parseFetchRecordLine(line string) (partition.Record, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return partition.Record{}, fmt.Errorf("invalid record line: %s", line)
	}

	offset, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return partition.Record{}, err
	}

	var key []byte
	if parts[1] != "nil" {
		key = []byte(parts[1])
	}

	value := []byte(parts[2])

	return partition.Record{
		Offset: offset,
		Key:    key,
		Value:  value,
	}, nil
}
