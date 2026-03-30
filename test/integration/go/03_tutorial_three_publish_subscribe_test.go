package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialThreePublishSubscribe(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	publisher := openChannel(t, conn)
	defer publisher.Close()

	consumerOne := openChannel(t, conn)
	defer consumerOne.Close()

	consumerTwo := openChannel(t, conn)
	defer consumerTwo.Close()

	exchange := unique("tutorial.three.logs")
	if err := publisher.ExchangeDeclare(exchange, "fanout", false, false, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare: %v", err)
	}

	queueOne, err := consumerOne.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare consumer one: %v", err)
	}
	queueTwo, err := consumerTwo.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare consumer two: %v", err)
	}

	if err := consumerOne.QueueBind(queueOne.Name, "", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind consumer one: %v", err)
	}
	if err := consumerTwo.QueueBind(queueTwo.Name, "", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind consumer two: %v", err)
	}

	msgsOne, err := consumerOne.Consume(queueOne.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume consumer one: %v", err)
	}
	msgsTwo, err := consumerTwo.Consume(queueTwo.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume consumer two: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := []byte("info: publish subscribe")
	if err := publisher.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	assertBodyReceived(t, ctx, msgsOne, string(body), "consumer one")
	assertBodyReceived(t, ctx, msgsTwo, string(body), "consumer two")
}
