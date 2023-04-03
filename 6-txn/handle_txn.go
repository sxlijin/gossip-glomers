package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TxnOp struct {
}

type TxnRequest struct {
	MsgId int      `json:"msg_id,omitempty"`
	Type  string   `json:"type"`
	Txn   [][3]any `json:"txn"`
}

type TxnResponse struct {
	InReplyTo int      `json:"msg_id,omitempty"`
	Type      string   `json:"type"`
	Txn       [][3]any `json:"txn"`
}

func makeTxnResponse(req *TxnRequest) TxnResponse {
	return TxnResponse{
		InReplyTo: req.MsgId,
		Type:      "txn_ok",
	}
}

func (ls *LocalStore) HandleTxn(msg maelstrom.Message) error {
	var req TxnRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	resp := makeTxnResponse(&req)

	return ls.n.Reply(msg, resp)
}
