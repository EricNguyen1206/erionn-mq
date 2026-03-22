package coordinator

import (
	"sync"
	"time"
)

type GroupState string

const (
	StateEmpty         GroupState = "Empty"
	StatePreparingReb  GroupState = "PreparingRebalance"
	StateCompletingReb GroupState = "CompletingRebalance"
	StateStable        GroupState = "Stable"
	StateDead          GroupState = "Dead"
)

type TopicPartition struct {
	Topic     string
	Partition int32
}

type Member struct {
	MemberID           string
	ClientID           string
	ClientHost         string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32

	ProtocolType string
	Protocols    []GroupProtocol // from JoinGroup

	Assignment []TopicPartition

	GenerationID  int32
	LastHeartbeat time.Time
	JoinedAt      time.Time
}

type GroupProtocol struct {
	Name     string
	Metadata []byte
}

type Group struct {
	mu sync.Mutex

	GroupID      string
	State        GroupState
	GenerationID int32
	ProtocolType string
	ProtocolName string

	LeaderID string

	Members map[string]*Member

	Assignments map[string][]TopicPartition

	CommittedOffsets map[string]map[int32]int64 // topic -> partition -> next offset
}
