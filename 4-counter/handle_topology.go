package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopologyRequest struct {
	MsgId   int64  `json:"msg_id,omitempty"`
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type TopologyResponse struct {
	InReplyTo int64  `json:"msg_id,omitempty"`
	Type      string `json:"type"`
}

func makeTopologyResponse(req *TopologyRequest) TopologyResponse {
	return TopologyResponse{
		InReplyTo: req.MsgId,
		Type:      "topology_ok",
	}
}

func (ls *LocalStore) HandleTopology(msg maelstrom.Message) error {
	ls.init.Done()

	var req TopologyRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeTopologyResponse(&req)

	return ls.n.Reply(msg, resp)
}
