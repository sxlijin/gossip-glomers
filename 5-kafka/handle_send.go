package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequest struct {
	MsgId int    `json:"msg_id,omitempty"`
	Type  string `json:"type"`
	Key   string `json:"key"`
	Msg   int    `json:"msg"`
}

type SendResponse struct {
	InReplyTo int    `json:"msg_id,omitempty"`
	Type      string `json:"type"`
	Offset    int    `json:"offset"`
}

func makeSendResponse(req *SendRequest) SendResponse {
	return SendResponse{
		InReplyTo: req.MsgId,
		Type:      "send_ok",
		Offset:    0,
	}
}

func (ls *LocalStore) HandleSend(msg maelstrom.Message) error {
	var req SendRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeSendResponse(&req)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	ls.db[req.Key] = append(ls.db[req.Key], req.Msg)
	resp.Offset = len(ls.db[req.Key]) - 1

	ls.l.Printf("RX/send %s=%d\n%#v\n%#v", req.Key, req.Msg, ls.db, resp)

	return ls.n.Reply(msg, resp)
}
