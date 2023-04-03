package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type PollRequest struct {
	MsgId   int            `json:"msg_id,omitempty"`
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	InReplyTo int                `json:"msg_id,omitempty"`
	Type      string             `json:"type"`
	Msgs      map[string][][]int `json:"msgs"`
}

func makePollResponse(req *PollRequest) PollResponse {
	return PollResponse{
		InReplyTo: req.MsgId,
		Type:      "poll_ok",
		Msgs:      make(map[string][][]int),
	}
}

func (ls *LocalStore) HandlePoll(msg maelstrom.Message) error {
	var req PollRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makePollResponse(&req)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	for key, offset := range req.Offsets {
		ret := resp.Msgs[key]
		for i, val := range ls.db[key][offset:] {
			ret = append(ret, []int{i + offset, val})
		}
		resp.Msgs[key] = ret
	}

	ls.l.Printf("RX/poll %#v\n%#v", req.Offsets, resp)

	return ls.n.Reply(msg, resp)
}
