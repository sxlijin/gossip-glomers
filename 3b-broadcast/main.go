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
	ls.l.SetFlags(log.Ltime | log.Lmicroseconds)

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

func (ls *LocalStore) Unlocked_ApplyWrite(val int64) {
	ls.db[val] = struct{}{}
}

func (ls *LocalStore) PollNeighbors(parentCtx context.Context) {
	ls.l.Printf("Attempting to poll neighbors")
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
	respBody["high_watermark"] = len(ls.db)
	respBody["db"] = c

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
