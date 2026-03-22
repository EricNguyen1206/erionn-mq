package handlers

import "erionn-mq/internal/coordinator"

type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string

	// MVP: decode member subscriptions from JoinGroup metadata and pass here
	Subscriptions map[string][]string
}

type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []coordinator.TopicPartition
}

type SyncGroupHandler struct {
	Coord *coordinator.GroupCoordinator
}

func (h *SyncGroupHandler) Handle(req *SyncGroupRequest) (*SyncGroupResponse, error) {
	assignment, err := h.Coord.SyncGroup(
		req.GroupID,
		req.MemberID,
		req.GenerationID,
		req.Subscriptions,
	)
	if err != nil {
		return nil, err
	}
	return &SyncGroupResponse{
		ErrorCode:  0,
		Assignment: assignment,
	}, nil
}
