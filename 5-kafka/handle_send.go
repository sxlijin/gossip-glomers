package main

import (
	"context"
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

	ctx := context.Background()

	pre := make([]int, 0)
	createIfNotExists := false
	if err := ls.kv.ReadInto(ctx, req.Key, &pre); err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			createIfNotExists = true
		} else {
			return err
		}
	}
	post := append(pre, req.Msg)

	if !createIfNotExists {
		pre = nil
	}

	if err := ls.kv.CompareAndSwap(ctx, req.Key, pre, post, createIfNotExists); err != nil {
		return err
	}

	// if (createIfNotExists) {
	// 	vals := make([]int, 0)
	// 	ls.kv.ReadInto(ctx, req.Key, &vals)
	// 	ls.l.Printf("LinKV/read %s -> %#v", req.Key, vals)
	// }

	ls.l.Printf("\nRX/send %s=%d\nTX/send %#v", req.Key, req.Msg, resp)

	return ls.n.Reply(msg, resp)
}
