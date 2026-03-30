package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegressionMultipleChannelsPerConnection(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.multi-ch")
	const numChannels = 20

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	channels := make([]*amqp.Channel, numChannels)
	for i := range channels {
		channels[i] = openChannel(t, conn)
	}

	for _, ch := range channels {
		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			if !strings.Contains(err.Error(), "already declared") {
				t.Fatalf("QueueDeclare on channel: %v", err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, ch := range channels {
		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: []byte("from-ch")}); err != nil {
			t.Fatalf("Publish on channel: %v", err)
		}
	}

	for _, ch := range channels {
		if err := ch.Close(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
	}

	ch := openChannel(t, conn)
	defer ch.Close()

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	var received int
	for received < numChannels {
		select {
		case msg := <-msgs:
			received++
			if err := msg.Ack(false); err != nil {
				t.Fatalf("Ack %d: %v", received, err)
			}
		case <-ctx.Done():
			t.Fatalf("timeout: received %d of %d messages", received, numChannels)
		}
	}
}
