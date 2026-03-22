package logstore

import (
	"sync"

	"github.com/EricNguyen1206/erionn-mq/internal/partition"
)

type MemoryLog struct {
	mu         sync.RWMutex
	records    []partition.Record
	lastOffset int64
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		records:    make([]partition.Record, 0),
		lastOffset: -1,
	}
}

func (m *MemoryLog) Append(record partition.Record) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastOffset++
	record.Offset = m.lastOffset

	record.Key = append([]byte(nil), record.Key...)
	record.Value = append([]byte(nil), record.Value...)

	m.records = append(m.records, record)
	return record.Offset, nil
}

func (m *MemoryLog) Read(offset int64, max int) ([]partition.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if max <= 0 {
		max = len(m.records)
	}

	out := make([]partition.Record, 0, max)
	for _, r := range m.records {
		if r.Offset < offset {
			continue
		}
		out = append(out, partition.Record{
			Offset:    r.Offset,
			Timestamp: r.Timestamp,
			Key:       append([]byte(nil), r.Key...),
			Value:     append([]byte(nil), r.Value...),
		})
		if len(out) >= max {
			break
		}
	}

	return out, nil
}

func (m *MemoryLog) LastOffset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastOffset
}
