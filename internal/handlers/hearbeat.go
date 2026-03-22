package handlers

import "erionn-mq/internal/coordinator"

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

type HeartbeatResponse struct {
	ErrorCode int16
}

type HeartbeatHandler struct {
	Coord *coordinator.GroupCoordinator
}

func (h *HeartbeatHandler) Handle(req *HeartbeatRequest) (*HeartbeatResponse, error) {
	if err := h.Coord.Heartbeat(req.GroupID, req.MemberID, req.GenerationID); err != nil {
		return nil, err
	}
	return &HeartbeatResponse{ErrorCode: 0}, nil
}
