package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalStore struct {
	n *maelstrom.Node
	l *log.Logger

	db   map[int64]struct{}
	dbMu sync.Mutex

	lwm   map[string]int64
	lwmMu sync.Mutex
}

func (ls *LocalStore) Run() error {
	// Maelstrom test handlers
	ls.n.Handle("broadcast", ls.HandleBroadcast)
	ls.n.Handle("read", ls.HandleRead)
	ls.n.Handle("topology", ls.HandleTopology)

	// Custom handlers
	ls.n.Handle("GetHighWatermark", ls.HandleGetHighWatermark)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
				ls.PollNeighbors(ctx)
			}
		}
	}()

	if err := ls.n.Run(); err != nil {
		ls.l.Fatal(err)
		return err
	}

	cancel()

	return nil
}

// Caller MUST lock ls.dbMu
func (ls *LocalStore) Lwm() int64 {
	return int64(len(ls.db))
}

func (ls *LocalStore) PollNeighbors(parentCtx context.Context) {
	ls.dbMu.Lock()
	ls.lwmMu.Lock()
	defer ls.lwmMu.Unlock()
	defer ls.dbMu.Unlock()

	type Update struct {
		Hwm int64   `json:"high_watermark"`
		Db  []int64 `json:"db"`
	}

	polls := make(map[string]Update)
	neighbors := ls.n.NodeIDs()
	ls.l.Printf("Polling neighbors (lwm=%d): %#v", ls.Lwm(), neighbors)

	var wg sync.WaitGroup
	wg.Add(len(neighbors))

	for _, neighborId := range neighbors {
		if ls.n.ID() == neighborId {
			wg.Done()
			continue
		}
		go func(dst string) {
			defer wg.Done()

			req := make(map[string]any)
			req["type"] = "GetHighWatermark"
			req["low_watermark"] = ls.Lwm()

			ctx, cancel := context.WithTimeout(parentCtx, 100*time.Millisecond)
			defer cancel()

			resp, err := ls.n.SyncRPC(ctx, dst, req)
			if err != nil {
				return
			}
			var respBody Update
			if err := json.Unmarshal(resp.Body, &respBody); err != nil {
				return
			}
			polls[dst] = respBody
		}(neighborId)
	}

	wg.Wait()

	for neighborId, update := range polls {
		if ls.lwm[neighborId] == update.Hwm {
			continue
		}
		ls.lwm[neighborId] = update.Hwm
		for _, val := range update.Db {
			ls.db[val] = struct{}{}
		}
	}
}

// TODO: implement described optimization
// If LWM == HWM, returns empty values slice
// If LWM != HWM, returns all values in local DB - cannot know which values client doesn't know about
func (ls *LocalStore) HandleGetHighWatermark(req maelstrom.Message) error {
	var reqBody struct {
		MsgId int64  `json:"msg_id"`
		Type  string `json:"type"`
	}
	if err := json.Unmarshal(req.Body, &reqBody); err != nil {
		return err
	}
	respBody := make(map[string]any)

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	c := make([]int64, 0)
	for val, _ := range ls.db {
		c = append(c, val)
	}
	respBody["in_reply_to"] = reqBody.MsgId
	respBody["type"] = "GetHighWatermark_ok"
	respBody["high_watermark"] = ls.Lwm()
	respBody["db"] = c

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

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	for _, dest := range ls.n.NodeIDs() {
		// if dest is self
		if dest == ls.n.ID() {
			continue
		}

		// need to make every target receive my high watermark
		//ls.n.Send(dest, make(map[string]any))
	}

	ls.db[reqBody.Message] = struct{}{}
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

	ls.dbMu.Lock()
	defer ls.dbMu.Unlock()

	c := make([]int64, 0)
	for val, _ := range ls.db {
		c = append(c, val)
	}
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
		n:   maelstrom.NewNode(),
		l:   log.Default(),
		db:  make(map[int64]struct{}),
		lwm: make(map[string]int64),
	}

	if err := ls.Run(); err != nil {
		log.Fatal(err)
	}
}
