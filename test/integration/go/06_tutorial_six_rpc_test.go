package integration_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialSixRPC(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	serverConn := connect(t, b.amqpURL)
	defer serverConn.Close()
	serverCh := openChannel(t, serverConn)
	defer serverCh.Close()

	clientConn := connect(t, b.amqpURL)
	defer clientConn.Close()
	clientCh := openChannel(t, clientConn)
	defer clientCh.Close()

	if _, err := serverCh.QueueDeclare("rpc_queue", true, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare rpc_queue: %v", err)
	}
	if err := serverCh.Qos(1, 0, false); err != nil {
		t.Fatalf("Qos: %v", err)
	}

	requests, err := serverCh.Consume("rpc_queue", "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume rpc_queue: %v", err)
	}

	replyQueue, err := clientCh.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare reply queue: %v", err)
	}
	replies, err := clientCh.Consume(replyQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume reply queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	correlationID := unique("tutorial.six.rpc")
	go func() {
		msg := <-requests
		n, err := strconv.Atoi(string(msg.Body))
		if err != nil {
			t.Errorf("strconv.Atoi: %v", err)
			return
		}

		response := strconv.Itoa(fib(n))
		err = serverCh.PublishWithContext(ctx, "", msg.ReplyTo, false, false, amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          []byte(response),
		})
		if err != nil {
			t.Errorf("Publish response: %v", err)
			return
		}
		if err := msg.Ack(false); err != nil {
			t.Errorf("Ack request: %v", err)
		}
	}()

	if err := clientCh.PublishWithContext(ctx, "", "rpc_queue", false, false, amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationID,
		ReplyTo:       replyQueue.Name,
		Body:          []byte("8"),
	}); err != nil {
		t.Fatalf("Publish request: %v", err)
	}

	select {
	case reply := <-replies:
		if reply.CorrelationId != correlationID {
			t.Fatalf("unexpected correlation id: got=%q want=%q", reply.CorrelationId, correlationID)
		}
		if string(reply.Body) != "21" {
			t.Fatalf("unexpected rpc body: got=%q want=%q", reply.Body, "21")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for rpc reply")
	}
}

func fib(n int) int {
	if n < 2 {
		return n
	}
	return fib(n-1) + fib(n-2)
}
