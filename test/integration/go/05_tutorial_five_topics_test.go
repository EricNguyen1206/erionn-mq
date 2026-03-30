package integration_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTutorialFiveTopics(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	publisher := openChannel(t, conn)
	defer publisher.Close()

	kernelConsumer := openChannel(t, conn)
	defer kernelConsumer.Close()

	criticalConsumer := openChannel(t, conn)
	defer criticalConsumer.Close()

	allConsumer := openChannel(t, conn)
	defer allConsumer.Close()

	exchange := unique("tutorial.five.topic")
	if err := publisher.ExchangeDeclare(exchange, "topic", false, false, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare: %v", err)
	}

	kernelQueue, err := kernelConsumer.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare kernel: %v", err)
	}
	criticalQueue, err := criticalConsumer.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare critical: %v", err)
	}
	allQueue, err := allConsumer.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare all: %v", err)
	}

	if err := kernelConsumer.QueueBind(kernelQueue.Name, "kern.*", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind kernel: %v", err)
	}
	if err := criticalConsumer.QueueBind(criticalQueue.Name, "*.critical", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind critical: %v", err)
	}
	if err := allConsumer.QueueBind(allQueue.Name, "#", exchange, false, nil); err != nil {
		t.Fatalf("QueueBind all: %v", err)
	}

	kernelMsgs, err := kernelConsumer.Consume(kernelQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume kernel: %v", err)
	}
	criticalMsgs, err := criticalConsumer.Consume(criticalQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume critical: %v", err)
	}
	allMsgs, err := allConsumer.Consume(allQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume all: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := publisher.PublishWithContext(ctx, exchange, "kern.critical", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("kernel critical"),
	}); err != nil {
		t.Fatalf("Publish kern.critical: %v", err)
	}
	assertBodyReceived(t, ctx, kernelMsgs, "kernel critical", "kernel consumer")
	assertBodyReceived(t, ctx, criticalMsgs, "kernel critical", "critical consumer")
	assertBodyReceived(t, ctx, allMsgs, "kernel critical", "all consumer")

	if err := publisher.PublishWithContext(ctx, exchange, "auth.info", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("auth info"),
	}); err != nil {
		t.Fatalf("Publish auth.info: %v", err)
	}
	assertNoMessage(t, kernelMsgs, "kernel consumer should not receive auth.info")
	assertNoMessage(t, criticalMsgs, "critical consumer should not receive auth.info")
	assertBodyReceived(t, ctx, allMsgs, "auth info", "all consumer second message")
}
