package main

import (
	"fmt"
	"log"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/logstore"
	"erionn-mq/internal/partition"
)

func main() {
	// Smoke test: broker produce & fetch using in-process API (no TCP server).
	b := broker.New()

	mem := logstore.NewMemoryLog()
	p := partition.New("orders", 0, mem)

	if err := b.CreateTopic("orders", []*partition.Partition{p}); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		offset, err := b.Produce("orders", 0, []byte("key"), []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("produced offset:", offset)
	}

	records, hw, err := b.Fetch("orders", 0, 0, 10)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("high watermark:", hw)
	for _, r := range records {
		fmt.Printf("record offset=%d value=%s\n", r.Offset, string(r.Value))
	}
}
