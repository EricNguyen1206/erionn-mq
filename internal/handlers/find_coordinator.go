package handlers

import "erionn-mq/internal/coordinator"

type FindCoordinatorHandler struct {
	Coord *coordinator.GroupCoordinator
}

type FindCoordinatorRequest struct {
	GroupID string
}

type FindCoordinatorResponse struct {
	ErrorCode int16
	NodeID    int32
	Host      string
	Port      int32
}

func (h *FindCoordinatorHandler) Handle(req *FindCoordinatorRequest) *FindCoordinatorResponse {
	nodeID, host, port := h.Coord.FindCoordinator(req.GroupID)
	return &FindCoordinatorResponse{
		ErrorCode: 0,
		NodeID:    nodeID,
		Host:      host,
		Port:      port,
	}
}
