package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ReadRequest struct {
	MsgId int    `json:"msg_id,omitempty"`
	Type  string `json:"type"`
}

type ReadResponse struct {
	InReplyTo int    `json:"msg_id,omitempty"`
	Type      string `json:"type"`
	Value     int    `json:"value"`
}

func makeReadResponse(req *ReadRequest) ReadResponse {
	return ReadResponse{
		InReplyTo: req.MsgId,
		Type:      "read_ok",
		Value:     0,
	}
}

func (ls *LocalStore) HandleRead(msg maelstrom.Message) error {
	var req ReadRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeReadResponse(&req)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	for _, val := range ls.db {
		resp.Value += val
	}

	return ls.n.Reply(msg, resp)
}
