package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/EricNguyen1206/erionn-mq/internal/partition"
)

const BROKER_PORT = 10000

const (
	ECHO = 1
)

var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
)

type Broker struct {
	listener net.Listener
	topics   map[string]map[int]*partition.Partition
	mu       sync.RWMutex
}

func New() *Broker {
	return &Broker{
		topics: make(map[string]map[int]*partition.Partition),
	}
}

func (b *Broker) CreateTopic(name string, partitions []*partition.Partition) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	pmap := make(map[int]*partition.Partition, len(partitions))
	for _, p := range partitions {
		pmap[p.ID()] = p
	}

	b.topics[name] = pmap

	return nil
}

func (b *Broker) AddPartition(topic string, p *partition.Partition) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; !ok {
		b.topics[topic] = make(map[int]*partition.Partition)
	}

	if _, exists := b.topics[topic][p.ID()]; exists {
		return fmt.Errorf("partition %d already exists in topic %s", p.ID(), topic)
	}

	b.topics[topic][p.ID()] = p
	return nil
}

func (b *Broker) GetPartition(topic string, partitionID int) (*partition.Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	partitions, ok := b.topics[topic]
	if !ok {
		return nil, ErrTopicNotFound
	}

	p, ok := partitions[partitionID]
	if !ok {
		return nil, ErrPartitionNotFound
	}

	return p, nil
}

func (b *Broker) Produce(topic string, partitionID int, key, value []byte) (int64, error) {
	p, err := b.GetPartition(topic, partitionID)
	if err != nil {
		return -1, err
	}

	return p.Produce(key, value)
}

func (b *Broker) Fetch(topic string, partitionID int, offset int64, max int) ([]partition.Record, int64, error) {
	p, err := b.GetPartition(topic, partitionID)
	if err != nil {
		return nil, 0, err
	}

	return p.Fetch(offset, max)
}
