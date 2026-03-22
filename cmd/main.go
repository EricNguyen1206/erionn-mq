package main

import (
	"fmt"
	"log"

	"erionn-mq/internal/client"
)

func main() {
	// 1. Producer gửi message
	producer, err := client.NewProducer("localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		offset, err := producer.Send("orders", 0, []byte("user-1"), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("produced offset:", offset)
	}

	// 2. Consumer đơn lẻ đọc từ đầu
	consumer1, err := client.NewConsumer("localhost:9092", "orders", 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer1.Close()

	records, hw, err := consumer1.Poll(10)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("high watermark:", hw)
	for _, r := range records {
		fmt.Printf("consumer record offset=%d value=%s\n", r.Offset, string(r.Value))
	}

	// 3. Consumer group
	group := consumer.NewConsumerGroup("group-1")

	if err := group.Join("member-1"); err != nil {
		log.Fatal(err)
	}
	if err := group.Join("member-2"); err != nil {
		log.Fatal(err)
	}

	partitions := []consumer.TopicPartition{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 1},
	}
	if err := group.Rebalance(partitions); err != nil {
		log.Fatal(err)
	}

	gc1 := consumer.NewGroupConsumer("localhost:9092", group, "member-1")
	defer gc1.Close()

	if err := gc1.InitAssignedConsumers(); err != nil {
		log.Fatal(err)
	}

	polled, err := gc1.Poll()
	if err != nil {
		log.Fatal(err)
	}

	for tp, records := range polled {
		fmt.Printf("member-1 assigned %s-%d\n", tp.Topic, tp.Partition)
		for _, r := range records {
			fmt.Printf("group record offset=%d value=%s\n", r.Offset, string(r.Value))
		}
	}

	gc1.Commit()

	fmt.Println("committed offset partition 0:", group.CommittedOffset("orders", 0))
}
