package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegressionConnectionCloseCleansUpChannels(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.conn-close")

	conn := connect(t, b.amqpURL)
	ch := openChannel(t, conn)
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Connection.Close: %v", err)
	}

	conn2 := connect(t, b.amqpURL)
	defer conn2.Close()
	ch2 := openChannel(t, conn2)
	defer ch2.Close()

	if _, err := ch2.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		if !strings.Contains(err.Error(), "already declared") {
			t.Fatalf("QueueDeclare after reconnect: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch2.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: []byte("ok")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgs, err := ch2.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "ok" {
			t.Fatalf("unexpected body: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout: publish/consume failed after connection close")
	}
}
