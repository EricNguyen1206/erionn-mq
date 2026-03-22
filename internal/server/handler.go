package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/partition"
)

type ConnectionHandler struct {
	broker *broker.Broker
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewConnectionHandler(b *broker.Broker, conn net.Conn) *ConnectionHandler {
	return &ConnectionHandler{
		broker: b,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (h *ConnectionHandler) Serve() error {
	for {
		line, err := h.reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return err
			}
			return fmt.Errorf("read command: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if err := h.handleLine(line); err != nil {
			if _, werr := fmt.Fprintf(h.writer, "ERR %v\n", err); werr != nil {
				return werr
			}
			if err := h.writer.Flush(); err != nil {
				return err
			}
		}
	}
}

func (h *ConnectionHandler) handleLine(line string) error {
	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		return errors.New("empty command")
	}

	switch strings.ToUpper(parts[0]) {
	case "PRODUCE":
		return h.handleProduce(parts)
	case "FETCH":
		return h.handleFetch(parts)
	case "PING":
		_, err := fmt.Fprintln(h.writer, "PONG")
		if err != nil {
			return err
		}
		return h.writer.Flush()
	default:
		return fmt.Errorf("unknown command %q", parts[0])
	}
}

func (h *ConnectionHandler) handleProduce(parts []string) error {
	if len(parts) < 5 {
		return errors.New("usage: PRODUCE <topic> <partition> <key> <value>")
	}

	topic := parts[1]

	partitionID, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("invalid partition: %w", err)
	}

	key := []byte(parts[3])
	value := []byte(strings.Join(parts[4:], " "))

	offset, err := h.broker.Produce(topic, partitionID, key, value)
	if err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}

	if _, err := fmt.Fprintf(h.writer, "OK %d\n", offset); err != nil {
		return err
	}

	return h.writer.Flush()
}

func (h *ConnectionHandler) handleFetch(parts []string) error {
	if len(parts) != 5 {
		return errors.New("usage: FETCH <topic> <partition> <offset> <max>")
	}

	topic := parts[1]

	partitionID, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("invalid partition: %w", err)
	}

	offset, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset: %w", err)
	}

	max, err := strconv.Atoi(parts[4])
	if err != nil {
		return fmt.Errorf("invalid max: %w", err)
	}

	records, hw, err := h.broker.Fetch(topic, partitionID, offset, max)
	if err != nil {
		return fmt.Errorf("fetch failed: %w", err)
	}

	if err := writeFetchResponse(h.writer, records, hw); err != nil {
		return err
	}

	return h.writer.Flush()
}

func writeFetchResponse(w *bufio.Writer, records []partition.Record, hw int64) error {
	if _, err := fmt.Fprintf(w, "MESSAGES %d HW %d\n", len(records), hw); err != nil {
		return err
	}

	for _, r := range records {
		key := "nil"
		if len(r.Key) > 0 {
			key = string(r.Key)
		}

		if _, err := fmt.Fprintf(w, "%d %s %s\n", r.Offset, key, string(r.Value)); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintln(w, "END")
	return err
}
