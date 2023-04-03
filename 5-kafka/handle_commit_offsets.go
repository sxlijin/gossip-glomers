package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type CommitOffsetsRequest struct {
	MsgId   int            `json:"msg_id,omitempty"`
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsResponse struct {
	InReplyTo int    `json:"msg_id,omitempty"`
	Type      string `json:"type"`
}

func makeCommitOffsetsResponse(req *CommitOffsetsRequest) CommitOffsetsResponse {
	return CommitOffsetsResponse{
		InReplyTo: req.MsgId,
		Type:      "commit_offsets_ok",
	}
}

func (ls *LocalStore) HandleCommitOffsets(msg maelstrom.Message) error {
	var req CommitOffsetsRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeCommitOffsetsResponse(&req)

	return ls.n.Reply(msg, resp)
}
