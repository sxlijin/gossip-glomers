package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TxnRequest struct {
	MsgId int      `json:"msg_id,omitempty"`
	Type  string   `json:"type"`
	Txn   [][3]any `json:"txn"`
}

func (ls *LocalStore) FromTxnRequest(req TxnRequest) []Op {
	ret := make([]Op, 0)

	for _, op := range req.Txn {
		switch op[0].(string) {
		case "r":
			ret = append(ret, Op{
				Type:  ReadOp,
				Key:   int(op[1].(float64)),
				Value: 0,
			})
		case "w":
			ret = append(ret, Op{
				Type:  WriteOp,
				Key:   int(op[1].(float64)),
				Value: int(op[2].(float64)),
			})
		default:
			ls.l.Printf("Unrecognized operation type in req %#v", req)
		}
	}

	return ret
}

type TxnResponse struct {
	InReplyTo int      `json:"msg_id,omitempty"`
	Type      string   `json:"type"`
	Txn       [][3]any `json:"txn"`
}

func makeTxnResponse(req *TxnRequest, ops []Op) TxnResponse {
	resp := TxnResponse{
		InReplyTo: req.MsgId,
		Type:      "txn_ok",
		Txn:       make([][3]any, 0),
	}

	for _, op := range ops {
		resp.Txn = append(resp.Txn, [3]any{op.Type, op.Key, op.Value})
	}
	return resp
}

func (ls *LocalStore) HandleTxn(msg maelstrom.Message) error {
	var req TxnRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	ops := ls.FromTxnRequest(req)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	ls.Unlocked_ApplyOps(ops)

	resp := makeTxnResponse(&req, ops)

	return ls.n.Reply(msg, resp)
}
