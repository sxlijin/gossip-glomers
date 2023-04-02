package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequest struct {
	MsgId   int64  `json:"msg_id,omitempty"`
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type AddResponse struct {
	InReplyTo int64  `json:"msg_id,omitempty"`
	Type      string `json:"type"`
}

func makeAddResponse(req *AddRequest) AddResponse {
	return AddResponse{
		InReplyTo: req.MsgId,
		Type:      "add_ok",
	}
}

func (ls *LocalStore) HandleAdd(msg maelstrom.Message) error {
	var req AddRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeAddResponse(&req)

	return ls.n.Reply(msg, resp)
}
