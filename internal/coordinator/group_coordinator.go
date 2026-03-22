package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrGroupNotFound       = errors.New("group not found")
	ErrMemberNotFound      = errors.New("member not found")
	ErrIllegalGeneration   = errors.New("illegal generation")
	ErrRebalanceInProgress = errors.New("rebalance in progress")
)

type MetadataProvider interface {
	TopicPartitions(topic string) []int32
}

type GroupCoordinator struct {
	mu sync.RWMutex

	nodeID int32
	host   string
	port   int32
	meta   MetadataProvider

	groups map[string]*Group
}

func NewGroupCoordinator(nodeID int32, host string, port int32, meta MetadataProvider) *GroupCoordinator {
	return &GroupCoordinator{
		nodeID: nodeID,
		host:   host,
		port:   port,
		meta:   meta,
		groups: make(map[string]*Group),
	}
}

func (c *GroupCoordinator) FindCoordinator(groupID string) (nodeID int32, host string, port int32) {
	return c.nodeID, c.host, c.port
}

func (c *GroupCoordinator) getOrCreateGroup(groupID string, protocolType string) *Group {
	g, ok := c.groups[groupID]
	if ok {
		return g
	}
	g = &Group{
		GroupID:          groupID,
		State:            StateEmpty,
		GenerationID:     0,
		ProtocolType:     protocolType,
		Members:          make(map[string]*Member),
		Assignments:      make(map[string][]TopicPartition),
		CommittedOffsets: make(map[string]map[int32]int64),
	}
	c.groups[groupID] = g
	return g
}

func (c *GroupCoordinator) JoinGroup(
	groupID, memberID, clientID, clientHost, protocolType string,
	sessionTimeoutMs, rebalanceTimeoutMs int32,
	protocols []GroupProtocol,
) (generationID int32, leaderID string, assignedMemberID string, members []*Member, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g := c.getOrCreateGroup(groupID, protocolType)

	g.mu.Lock()
	defer g.mu.Unlock()

	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", clientID, time.Now().UnixNano())
	}

	if _, ok := g.Members[memberID]; !ok {
		g.Members[memberID] = &Member{
			MemberID:           memberID,
			ClientID:           clientID,
			ClientHost:         clientHost,
			SessionTimeoutMs:   sessionTimeoutMs,
			RebalanceTimeoutMs: rebalanceTimeoutMs,
			ProtocolType:       protocolType,
			Protocols:          protocols,
			JoinedAt:           time.Now(),
			LastHeartbeat:      time.Now(),
		}
	}

	g.GenerationID++
	g.State = StatePreparingReb

	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	if len(memberIDs) > 0 {
		g.LeaderID = memberIDs[0]
	}
	g.ProtocolName = "roundrobin"
	g.State = StateCompletingReb

	memberList := make([]*Member, 0, len(memberIDs))
	for _, id := range memberIDs {
		m := g.Members[id]
		m.GenerationID = g.GenerationID
		memberList = append(memberList, m)
	}

	return g.GenerationID, g.LeaderID, memberID, memberList, nil
}

func (c *GroupCoordinator) SyncGroup(
	groupID, memberID string,
	generationID int32,
	subscriptions map[string][]string,
) ([]TopicPartition, error) {
	c.mu.RLock()
	g, ok := c.groups[groupID]
	c.mu.RUnlock()
	if !ok {
		return nil, ErrGroupNotFound
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.GenerationID != generationID {
		return nil, ErrIllegalGeneration
	}
	if _, ok := g.Members[memberID]; !ok {
		return nil, ErrMemberNotFound
	}

	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}

	assignments := BuildRoundRobinAssignment(memberIDs, subscriptions, c.meta)
	g.Assignments = assignments
	g.State = StateStable

	member := g.Members[memberID]
	member.Assignment = assignments[memberID]
	member.LastHeartbeat = time.Now()

	return assignments[memberID], nil
}

func (c *GroupCoordinator) Heartbeat(groupID, memberID string, generationID int32) error {
	c.mu.RLock()
	g, ok := c.groups[groupID]
	c.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.GenerationID != generationID {
		return ErrIllegalGeneration
	}
	member, ok := g.Members[memberID]
	if !ok {
		return ErrMemberNotFound
	}
	member.LastHeartbeat = time.Now()
	return nil
}

func (c *GroupCoordinator) CommitOffset(
	groupID, memberID string,
	generationID int32,
	topic string,
	partition int32,
	offset int64,
) error {
	c.mu.RLock()
	g, ok := c.groups[groupID]
	c.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.GenerationID != generationID {
		return ErrIllegalGeneration
	}
	if _, ok := g.Members[memberID]; !ok {
		return ErrMemberNotFound
	}

	g.CommitOffset(topic, partition, offset)
	return nil
}

func (c *GroupCoordinator) FetchOffset(groupID, topic string, partition int32) (int64, bool, error) {
	c.mu.RLock()
	g, ok := c.groups[groupID]
	c.mu.RUnlock()
	if !ok {
		return 0, false, ErrGroupNotFound
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	offset, ok := g.FetchOffset(topic, partition)
	return offset, ok, nil
}

func (c *GroupCoordinator) EvictExpiredMembers(now time.Time) {
	c.mu.RLock()
	groups := make([]*Group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.RUnlock()

	for _, g := range groups {
		g.mu.Lock()

		removed := false
		for memberID, m := range g.Members {
			deadline := m.LastHeartbeat.Add(time.Duration(m.SessionTimeoutMs) * time.Millisecond)
			if now.After(deadline) {
				delete(g.Members, memberID)
				delete(g.Assignments, memberID)
				removed = true
			}
		}

		if removed {
			g.GenerationID++
			if len(g.Members) == 0 {
				g.State = StateEmpty
				g.LeaderID = ""
			} else {
				g.State = StatePreparingReb
			}
		}

		g.mu.Unlock()
	}
}
