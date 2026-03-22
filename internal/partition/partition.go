package partition

import (
	"errors"
	"sync"
	"time"
)

type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

type LogStore interface {
	Append(record Record) (int64, error)
	Read(offset int64, max int) ([]Record, error)
	LastOffset() int64
}

type Partition struct {
	topic string
	id    int

	mu sync.Mutex

	log LogStore
}

func New(topic string, id int, log LogStore) *Partition {
	return &Partition{
		topic: topic,
		id:    id,
		log:   log,
	}
}

func (p *Partition) Produce(key, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	record := Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       key,
		Value:     value,
	}

	return p.log.Append(record)
}

func (p *Partition) ProduceBatch(records []Record) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastOffset int64

	for i := range records {
		records[i].Timestamp = time.Now().UnixMilli()

		offset, err := p.log.Append(records[i])
		if err != nil {
			return -1, err
		}

		lastOffset = offset
	}

	return lastOffset, nil
}

func (p *Partition) Fetch(offset int64, max int) ([]Record, int64, error) {
	if offset < 0 {
		return nil, 0, errors.New("invalid offset")
	}

	records, err := p.log.Read(offset, max)
	if err != nil {
		return nil, 0, err
	}

	hw := p.log.LastOffset()

	return records, hw, nil
}

func (p *Partition) LastOffset() int64 {
	return p.log.LastOffset()
}

func (p *Partition) ID() int {
	return p.id
}

func (p *Partition) Topic() string {
	return p.topic
}
