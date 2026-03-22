package consumer

import (
	"fmt"

	"github.com/EricNguyen1206/erionn-mq/internal/client"
	"github.com/EricNguyen1206/erionn-mq/internal/partition"
)

type GroupConsumer struct {
	Group    *ConsumerGroup
	MemberID string
	Addr     string

	consumers map[TopicPartition]*client.Consumer
}

func NewGroupConsumer(addr string, group *ConsumerGroup, memberID string) *GroupConsumer {
	return &GroupConsumer{
		Group:     group,
		MemberID:  memberID,
		Addr:      addr,
		consumers: make(map[TopicPartition]*client.Consumer),
	}
}

func (gc *GroupConsumer) InitAssignedConsumers() error {
	assignments, err := gc.Group.Assignments(gc.MemberID)
	if err != nil {
		return err
	}

	for _, tp := range assignments {
		if _, exists := gc.consumers[tp]; exists {
			continue
		}

		offset := gc.Group.CommittedOffset(tp.Topic, tp.Partition)

		c, err := client.NewConsumer(gc.Addr, tp.Topic, tp.Partition, offset)
		if err != nil {
			return err
		}

		gc.consumers[tp] = c
	}

	return nil
}

func (gc *GroupConsumer) Close() error {
	var firstErr error
	for _, c := range gc.consumers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (gc *GroupConsumer) Poll() (map[TopicPartition][]partition.Record, error) {
	result := make(map[TopicPartition][]partition.Record)

	for tp, c := range gc.consumers {
		records, _, err := c.Poll(100)
		if err != nil {
			return nil, fmt.Errorf("poll %s-%d failed: %w", tp.Topic, tp.Partition, err)
		}

		if len(records) > 0 {
			result[tp] = records
		}
	}

	return result, nil
}

func (gc *GroupConsumer) Commit() {
	for tp, c := range gc.consumers {
		gc.Group.CommitOffset(tp.Topic, tp.Partition, c.CurrentOffset())
	}
}
