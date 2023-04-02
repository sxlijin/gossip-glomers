package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ReadRequest struct {
	MsgId   int64  `json:"msg_id,omitempty"`
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type ReadResponse struct {
	InReplyTo int64   `json:"msg_id,omitempty"`
	Type      string  `json:"type"`
	Messages  []int64 `json:"messages"`
}

func makeReadResponse(req *ReadRequest) ReadResponse {
	return ReadResponse{
		InReplyTo: req.MsgId,
		Type:      "read_ok",
		Messages:  make([]int64, 0),
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

	for val, _ := range ls.db {
		resp.Messages = append(resp.Messages, val)
	}

	return ls.n.Reply(msg, resp)
}
