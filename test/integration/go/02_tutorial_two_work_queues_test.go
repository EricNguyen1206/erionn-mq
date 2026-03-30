package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialTwoWorkQueues(t *testing.T) {
	t.Run("persistent task survives broker restart", func(t *testing.T) {
		b := startBroker(t)
		defer b.stop(t)

		queueName := unique("tutorial.two.tasks")

		conn := connect(t, b.amqpURL)
		ch := openChannel(t, conn)
		if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("durable task"),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		_ = ch.Close()
		_ = conn.Close()

		b.restart(t)

		conn = connect(t, b.amqpURL)
		defer conn.Close()
		ch = openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare after restart: %v", err)
		}

		msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		select {
		case msg := <-msgs:
			if string(msg.Body) != "durable task" {
				t.Fatalf("unexpected body after restart: %q", msg.Body)
			}
			if err := msg.Ack(false); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for durable task after restart")
		}
	})

	t.Run("unacked task is redelivered to another worker", func(t *testing.T) {
		b := startBroker(t)
		defer b.stop(t)

		queueName := unique("tutorial.two.redelivery")

		publisherConn := connect(t, b.amqpURL)
		defer publisherConn.Close()
		publisher := openChannel(t, publisherConn)
		defer publisher.Close()

		if _, err := publisher.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := publisher.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("important task"),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		workerOneConn := connect(t, b.amqpURL)
		workerOne := openChannel(t, workerOneConn)
		if err := workerOne.Qos(1, 0, false); err != nil {
			t.Fatalf("Qos worker one: %v", err)
		}

		firstMsgs, err := workerOne.Consume(queueName, "worker-one", false, false, false, false, nil)
		if err != nil {
			t.Fatalf("Consume worker one: %v", err)
		}

		var first amqp.Delivery
		select {
		case first = <-firstMsgs:
			if string(first.Body) != "important task" {
				t.Fatalf("unexpected first body: %q", first.Body)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for first worker delivery")
		}

		if err := workerOneConn.Close(); err != nil {
			t.Fatalf("worker one close: %v", err)
		}

		workerTwoConn := connect(t, b.amqpURL)
		defer workerTwoConn.Close()
		workerTwo := openChannel(t, workerTwoConn)
		defer workerTwo.Close()

		if err := workerTwo.Qos(1, 0, false); err != nil {
			t.Fatalf("Qos worker two: %v", err)
		}

		secondMsgs, err := workerTwo.Consume(queueName, "worker-two", false, false, false, false, nil)
		if err != nil {
			t.Fatalf("Consume worker two: %v", err)
		}

		select {
		case msg := <-secondMsgs:
			if !msg.Redelivered {
				t.Fatal("expected task to be redelivered after worker disconnect")
			}
			if string(msg.Body) != "important task" {
				t.Fatalf("unexpected redelivered body: %q", msg.Body)
			}
			if err := msg.Ack(false); err != nil {
				t.Fatalf("Ack worker two: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for redelivered task")
		}
	})

	t.Run("prefetch one keeps second task until first is acked", func(t *testing.T) {
		b := startBroker(t)
		defer b.stop(t)

		queueName := unique("tutorial.two.prefetch")

		conn := connect(t, b.amqpURL)
		defer conn.Close()
		ch := openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}
		if err := ch.Qos(1, 0, false); err != nil {
			t.Fatalf("Qos: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for _, body := range []string{"first task", "second task"} {
			if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			}); err != nil {
				t.Fatalf("Publish %q: %v", body, err)
			}
		}

		msgs, err := ch.Consume(queueName, "worker", false, false, false, false, nil)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		var first amqp.Delivery
		select {
		case first = <-msgs:
			if string(first.Body) != "first task" {
				t.Fatalf("unexpected first task body: %q", first.Body)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for first task")
		}

		select {
		case msg := <-msgs:
			t.Fatalf("received %q before first task was acked", msg.Body)
		case <-time.After(300 * time.Millisecond):
		}

		if err := first.Ack(false); err != nil {
			t.Fatalf("Ack first task: %v", err)
		}

		select {
		case msg := <-msgs:
			if string(msg.Body) != "second task" {
				t.Fatalf("unexpected second task body: %q", msg.Body)
			}
			if err := msg.Ack(false); err != nil {
				t.Fatalf("Ack second task: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for second task after first ack")
		}
	})
}
