package handlers

import "erionn-mq/internal/coordinator"

type JoinGroupRequest struct {
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	ProtocolType       string
	Protocols          []coordinator.GroupProtocol
	ClientID           string
	ClientHost         string
}

type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

type JoinGroupResponse struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	LeaderID     string
	MemberID     string
	Members      []JoinGroupMember
}

type JoinGroupHandler struct {
	Coord *coordinator.GroupCoordinator
}

func (h *JoinGroupHandler) Handle(req *JoinGroupRequest) (*JoinGroupResponse, error) {
	genID, leaderID, memberID, members, err := h.Coord.JoinGroup(
		req.GroupID,
		req.MemberID,
		req.ClientID,
		req.ClientHost,
		req.ProtocolType,
		req.SessionTimeoutMs,
		req.RebalanceTimeoutMs,
		req.Protocols,
	)
	if err != nil {
		return nil, err
	}

	respMembers := make([]JoinGroupMember, 0, len(members))
	for _, m := range members {
		var md []byte
		if len(m.Protocols) > 0 {
			md = m.Protocols[0].Metadata
		}
		respMembers = append(respMembers, JoinGroupMember{
			MemberID: m.MemberID,
			Metadata: md,
		})
	}

	return &JoinGroupResponse{
		ErrorCode:    0,
		GenerationID: genID,
		ProtocolName: "roundrobin",
		LeaderID:     leaderID,
		MemberID:     memberID,
		Members:      respMembers,
	}, nil
}
