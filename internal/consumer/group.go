package consumer

import (
	"fmt"
	"sort"
	"sync"
)

type TopicPartition struct {
	Topic     string
	Partition int
}

type Member struct {
	ID string
}

type ConsumerGroup struct {
	mu sync.Mutex

	GroupID string

	members map[string]*Member

	// memberID -> assigned partitions
	assignments map[string][]TopicPartition

	// committed offsets:
	// topic -> partition -> offset
	offsets map[string]map[int]int64
}

func NewConsumerGroup(groupID string) *ConsumerGroup {
	return &ConsumerGroup{
		GroupID:     groupID,
		members:     make(map[string]*Member),
		assignments: make(map[string][]TopicPartition),
		offsets:     make(map[string]map[int]int64),
	}
}

func (g *ConsumerGroup) Join(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.members[memberID]; exists {
		return fmt.Errorf("member %s already joined", memberID)
	}

	g.members[memberID] = &Member{ID: memberID}
	g.assignments[memberID] = nil
	return nil
}

func (g *ConsumerGroup) Leave(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.members[memberID]; !exists {
		return fmt.Errorf("member %s not found", memberID)
	}

	delete(g.members, memberID)
	delete(g.assignments, memberID)
	return nil
}

func (g *ConsumerGroup) Members() []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make([]string, 0, len(g.members))
	for id := range g.members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (g *ConsumerGroup) Rebalance(partitions []TopicPartition) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	if len(memberIDs) == 0 {
		return fmt.Errorf("no members to assign")
	}

	for _, memberID := range memberIDs {
		g.assignments[memberID] = nil
	}

	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic == partitions[j].Topic {
			return partitions[i].Partition < partitions[j].Partition
		}
		return partitions[i].Topic < partitions[j].Topic
	})

	for i, tp := range partitions {
		memberID := memberIDs[i%len(memberIDs)]
		g.assignments[memberID] = append(g.assignments[memberID], tp)
	}

	return nil
}

func (g *ConsumerGroup) Assignments(memberID string) ([]TopicPartition, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	assignments, ok := g.assignments[memberID]
	if !ok {
		return nil, fmt.Errorf("member %s not found", memberID)
	}

	out := make([]TopicPartition, len(assignments))
	copy(out, assignments)
	return out, nil
}

func (g *ConsumerGroup) CommitOffset(topic string, partition int, offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.offsets[topic]; !ok {
		g.offsets[topic] = make(map[int]int64)
	}

	g.offsets[topic][partition] = offset
}

func (g *ConsumerGroup) CommittedOffset(topic string, partition int) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	partitions, ok := g.offsets[topic]
	if !ok {
		return 0
	}

	offset, ok := partitions[partition]
	if !ok {
		return 0
	}

	return offset
}
