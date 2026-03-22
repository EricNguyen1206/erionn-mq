package handlers

import "erionn-mq/internal/coordinator"

type OffsetCommitPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Partitions   []OffsetCommitPartition
}

type OffsetCommitResponse struct {
	ErrorCodeByTP map[string]map[int32]int16
}

type OffsetCommitHandler struct {
	Coord *coordinator.GroupCoordinator
}

func (h *OffsetCommitHandler) Handle(req *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	resp := &OffsetCommitResponse{
		ErrorCodeByTP: make(map[string]map[int32]int16),
	}

	for _, p := range req.Partitions {
		if _, ok := resp.ErrorCodeByTP[p.Topic]; !ok {
			resp.ErrorCodeByTP[p.Topic] = make(map[int32]int16)
		}
		err := h.Coord.CommitOffset(req.GroupID, req.MemberID, req.GenerationID, p.Topic, p.Partition, p.Offset)
		if err != nil {
			resp.ErrorCodeByTP[p.Topic][p.Partition] = 1
			continue
		}
		resp.ErrorCodeByTP[p.Topic][p.Partition] = 0
	}

	return resp, nil
}
