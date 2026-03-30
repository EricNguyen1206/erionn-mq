package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegressionDurableRestart(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.durable")

	func() {
		conn := connect(t, b.amqpURL)
		defer conn.Close()
		ch := openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte("persisted"),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}()

	b.restart(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare after restart: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case msg := <-msgs:
		if string(msg.Body) != "persisted" {
			t.Fatalf("unexpected body after restart: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout: durable message did not survive restart")
	}
}
