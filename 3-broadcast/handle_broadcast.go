package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastRequest struct {
	MsgId   int64  `json:"msg_id,omitempty"`
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type BroadcastResponse struct {
	InReplyTo int64  `json:"msg_id,omitempty"`
	Type      string `json:"type"`
}

func makeBroadcastResponse(req *BroadcastRequest) BroadcastResponse {
	return BroadcastResponse{
		InReplyTo: req.MsgId,
		Type:      "broadcast_ok",
	}
}

func (ls *LocalStore) HandleBroadcast(msg maelstrom.Message) error {
	var req BroadcastRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeBroadcastResponse(&req)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	ls.Unlocked_ApplyWrite(req.Message)
	return ls.n.Reply(msg, resp)
}
