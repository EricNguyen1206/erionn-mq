package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegressionMultipleConcurrentConnections(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.multi-conn")
	const numConns = 10

	conns := make([]*amqp.Connection, numConns)
	for i := range conns {
		conns[i] = connect(t, b.amqpURL)
	}

	for _, conn := range conns {
		ch := openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			if !strings.Contains(err.Error(), "already declared") {
				t.Fatalf("QueueDeclare on conn: %v", err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: []byte("from-conn")}); err != nil {
			t.Fatalf("Publish on conn: %v", err)
		}
	}

	for i, conn := range conns {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn %d: %v", i, err)
		}
	}

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	var received int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for received < numConns {
		select {
		case msg := <-msgs:
			received++
			if err := msg.Ack(false); err != nil {
				t.Fatalf("Ack %d: %v", received, err)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: received %d of %d messages", received, numConns)
		}
	}
}
