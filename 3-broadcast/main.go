package main

import (
	"encoding/json"
	// "fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AtomicVal struct {
	val []int64
	mu  sync.Mutex
}

type LocalStore struct {
	n *maelstrom.Node
	l *log.Logger

	db []int64
	mu sync.Mutex
}

func (ls *LocalStore) Run() error {
	ls.n.Handle("broadcast", ls.HandleBroadcast)
	ls.n.Handle("read", ls.HandleRead)
	ls.n.Handle("topology", ls.HandleTopology)

	if err := ls.n.Run(); err != nil {
		ls.l.Fatal(err)
		return err
	}

	return nil
}

func (ls *LocalStore) HandleBroadcast(req maelstrom.Message) error {
	var reqBody struct {
		MsgId   int64  `json:"msg_id"`
		Type    string `json:"type"`
		Message int64  `json:"message"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	respBody := make(map[string]any)

	ls.mu.Lock()
	defer ls.mu.Unlock()

	for _, dest := range ls.n.NodeIDs() {
		// if dest is self
		if dest == ls.n.ID() {
			continue
		}

		// need to make every target receive my high watermark
		//ls.n.Send(dest, make(map[string]any))
	}

	ls.db = append(ls.db, reqBody.Message)
	respBody["msg_id"] = reqBody.MsgId
	respBody["type"] = "broadcast_ok"

	return ls.n.Reply(req, respBody)
}

func (ls *LocalStore) HandleRead(req maelstrom.Message) error {
	var reqBody map[string]any
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	respBody := make(map[string]any)

	ls.mu.Lock()
	defer ls.mu.Unlock()

	c := make([]int64, len(ls.db))
	copy(c, ls.db)
	respBody["type"] = "read_ok"
	respBody["msg_id"] = reqBody["msg_id"]
	respBody["messages"] = c

	return ls.n.Reply(req, respBody)
}

func (ls *LocalStore) HandleTopology(req maelstrom.Message) error {
	var reqBody map[string]any
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	respBody := make(map[string]any)

	respBody["type"] = "topology_ok"
	respBody["msg_id"] = reqBody["msg_id"]

	return ls.n.Reply(req, respBody)
}

func main() {
	ls := LocalStore{
		n: maelstrom.NewNode(),
		l: log.Default(),
	}

	if err := ls.Run(); err != nil {
		log.Fatal(err)
	}
}
