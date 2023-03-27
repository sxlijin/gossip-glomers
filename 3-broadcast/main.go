package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

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

	watermarksByNodeId map[string]int64
	watermarksMu       sync.Mutex
}

func (ls *LocalStore) Run() error {
	// Maelstrom test handlers
	ls.n.Handle("broadcast", ls.HandleBroadcast)
	ls.n.Handle("read", ls.HandleRead)
	ls.n.Handle("topology", ls.HandleTopology)

	// Custom handlers
	ls.n.Handle("GetHighWatermark", ls.HandleGetHighWatermark)

	if err := ls.n.Run(); err != nil {
		ls.l.Fatal(err)
		return err
	}

	return nil
}

func (ls *LocalStore) PollNeighbors() {
	ls.watermarksMu.Lock()
	defer ls.watermarksMu.Unlock()

	highWatermarks = make(map[string]int64)
	for neighborId, lwm := range ls.watermarksByNodeId {
		highWatermarks[neighborId] = lwm
	}
	for _, neighborId := range ls.n.NodeIDs() {
		go func() {
			req := make(map[string]any)
			req["type"] = "GetHighWatermark"

			resp, err := ls.n.SyncRPC(context.WithTimeout(context.Background(), 100*time.Millisecond), neighborId, req)
			if err {
				return
			}
			var respBody struct {
				highWatermark int64 `json:"high_watermark"`
			}
			if err := json.Unmarshal(resp.Body, &respBody); err != nil {
				return
			}
			highWatermarks[neighborId] = respBody.highWatermark
		}()
	}

	// TODO: update watermarksByNodeId using highWatermarks
	// TODO: update local DB with the write requests from the other nodes
	// TODO: need to figure out: are values unique? (non-unique values make this _much_ harder)
}

func (ls *LocalStore) HandleGetHighWatermark(req maelstrom.Message) error {
	var reqBody struct {
		MsgId int64  `json:""`
		Type  string `json:"type"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	respBody := make(map[string]any)

	ls.mu.Lock()
	defer ls.mu.Unlock()

	respBody["in_reply_to"] = reqBody.MsgId
	respBody["type"] = "GetHighWatermark_ok"
	respBody["high_watermark"] = len(ls.db)

	return ls.n.Reply(req, respBody)
}

func (ls *LocalStore) HandleBroadcast(req maelstrom.Message) error {
	var reqBody struct {
		MsgId   int64  `json:""`
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
	respBody["type"] = "broadcast_ok"
	respBody["in_reply_to"] = reqBody.MsgId

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
	respBody["in_reply_to"] = reqBody["msg_id"]
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
	respBody["in_reply_to"] = reqBody["msg_id"]

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
