package metadata

import "sync"

type Catalog struct {
	mu     sync.RWMutex
	topics map[string]int32 // topic -> partition count
}

func NewCatalog() *Catalog {
	return &Catalog{topics: make(map[string]int32)}
}

func (c *Catalog) AddTopic(topic string, partitions int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics[topic] = partitions
}

func (c *Catalog) TopicPartitions(topic string) []int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n := c.topics[topic]
	out := make([]int32, 0, n)
	for i := int32(0); i < n; i++ {
		out = append(out, i)
	}
	return out
}
