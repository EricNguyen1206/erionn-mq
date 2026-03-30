package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialOneHelloWorld(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	publisher := openChannel(t, conn)
	defer publisher.Close()

	consumer := openChannel(t, conn)
	defer consumer.Close()

	queueName := unique("tutorial.one.hello")
	q, err := publisher.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	msgs, err := consumer.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := []byte("Hello World!")
	if err := publisher.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != string(body) {
			t.Fatalf("unexpected body: got=%q want=%q", msg.Body, body)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for hello world message")
	}
}
