package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialFourRouting(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	publisher := openChannel(t, conn)
	defer publisher.Close()

	infoConsumer := openChannel(t, conn)
	defer infoConsumer.Close()

	errorConsumer := openChannel(t, conn)
	defer errorConsumer.Close()

	exchange := unique("tutorial.four.direct")
	if err := publisher.ExchangeDeclare(exchange, "direct", false, false, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare: %v", err)
	}

	infoQueue, err := infoConsumer.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare info queue: %v", err)
	}
	errorQueue, err := errorConsumer.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare error queue: %v", err)
	}

	if err := infoConsumer.QueueBind(infoQueue.Name, "info", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind info: %v", err)
	}
	if err := errorConsumer.QueueBind(errorQueue.Name, "error", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind error: %v", err)
	}

	infoMsgs, err := infoConsumer.Consume(infoQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume info: %v", err)
	}
	errorMsgs, err := errorConsumer.Consume(errorQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := publisher.PublishWithContext(ctx, exchange, "info", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("info message"),
	}); err != nil {
		t.Fatalf("Publish info: %v", err)
	}
	assertBodyReceived(t, ctx, infoMsgs, "info message", "info consumer")
	assertNoMessage(t, errorMsgs, "error consumer should not receive info severity")

	if err := publisher.PublishWithContext(ctx, exchange, "error", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("error message"),
	}); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	assertBodyReceived(t, ctx, errorMsgs, "error message", "error consumer")
	assertNoMessage(t, infoMsgs, "info consumer should not receive error severity")
}
