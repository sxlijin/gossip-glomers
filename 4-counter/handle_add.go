package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequest struct {
	MsgId int    `json:"msg_id,omitempty"`
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddResponse struct {
	InReplyTo int    `json:"msg_id,omitempty"`
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

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	ctx := context.Background()

	ls.db[ls.n.ID()] += req.Delta
	ls.kv.Write(ctx, ls.n.ID(), ls.db[ls.n.ID()])

	return ls.n.Reply(msg, resp)
}
