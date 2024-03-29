package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ListCommittedOffsetsRequest struct {
	MsgId int      `json:"msg_id,omitempty"`
	Type  string   `json:"type"`
	Keys  []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	InReplyTo int            `json:"msg_id,omitempty"`
	Type      string         `json:"type"`
	Offsets   map[string]int `json:"offsets"`
}

func makeListCommittedOffsetsResponse(req *ListCommittedOffsetsRequest) ListCommittedOffsetsResponse {
	return ListCommittedOffsetsResponse{
		InReplyTo: req.MsgId,
		Type:      "list_committed_offsets_ok",
		Offsets:   make(map[string]int),
	}
}

func (ls *LocalStore) HandleListCommittedOffsets(msg maelstrom.Message) error {
	var req ListCommittedOffsetsRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeListCommittedOffsetsResponse(&req)

	for _, key := range req.Keys {
		vals := make([]int, 0)
		if err := ls.kv.ReadInto(context.Background(), key, &vals); err != nil {
			return err
		}
		resp.Offsets[key] = len(vals)
	}

	return ls.n.Reply(msg, resp)
}
