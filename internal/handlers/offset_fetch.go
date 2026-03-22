package handlers

import "erionn-mq/internal/coordinator"

type OffsetFetchPartition struct {
	Topic     string
	Partition int32
}

type OffsetFetchRequest struct {
	GroupID    string
	Partitions []OffsetFetchPartition
}

type OffsetFetchItem struct {
	Topic     string
	Partition int32
	Offset    int64
	ErrorCode int16
}

type OffsetFetchResponse struct {
	Items []OffsetFetchItem
}

type OffsetFetchHandler struct {
	Coord *coordinator.GroupCoordinator
}

func (h *OffsetFetchHandler) Handle(req *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	out := &OffsetFetchResponse{
		Items: make([]OffsetFetchItem, 0, len(req.Partitions)),
	}

	for _, p := range req.Partitions {
		offset, ok, err := h.Coord.FetchOffset(req.GroupID, p.Topic, p.Partition)
		if err != nil {
			out.Items = append(out.Items, OffsetFetchItem{
				Topic: p.Topic, Partition: p.Partition, Offset: -1, ErrorCode: 1,
			})
			continue
		}
		if !ok {
			offset = -1
		}
		out.Items = append(out.Items, OffsetFetchItem{
			Topic: p.Topic, Partition: p.Partition, Offset: offset, ErrorCode: 0,
		})
	}

	return out, nil
}
