package integration_test

import "testing"

func TestRegressionChannelCloseCleansUpConsumers(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.ch-close")

	conn := connect(t, b.amqpURL)
	defer conn.Close()

	ch := openChannel(t, conn)
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	if _, err := ch.Consume(queue, "closer", false, false, false, false, nil); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := ch.Close(); err != nil {
		t.Fatalf("Channel.Close: %v", err)
	}

	ch2 := openChannel(t, conn)
	defer ch2.Close()

	if _, err := ch2.Consume(queue, "closer", false, false, false, false, nil); err != nil {
		t.Fatalf("Consume with same tag after channel close: %v", err)
	}
}
