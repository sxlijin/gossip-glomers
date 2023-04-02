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

	init sync.WaitGroup

	db   map[int64]struct{}
	dbMu sync.Mutex

	lwm   map[string]int64
	lwmMu sync.Mutex
}

func (ls *LocalStore) Run() error {
	ls.l.SetFlags(log.Ltime | log.Lmicroseconds)

	// Hook into init, but rely on the built-in response handler
	ls.init.Add(1)
	ls.n.Handle("init", func(msg maelstrom.Message) error {
		ls.init.Done()
		return nil
	})

	// Maelstrom test handlers
	ls.n.Handle("add", ls.HandleAdd)
	ls.n.Handle("read", ls.HandleRead)

	ctx, cancel := context.WithCancel(context.Background())
	go ls.ManagePollers(ctx)

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

func (ls *LocalStore) ManagePollers(ctx context.Context) {
	ls.init.Wait()

	pollers := make(map[string]struct{})
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
			for _, id := range ls.n.NodeIDs() {
				if id == ls.n.ID() {
					continue
				}
				if _, present := pollers[id]; !present {
					go ls.PollNode(ctx, id)
					pollers[id] = struct{}{}
				}
			}
		}
	}
}

func (ls *LocalStore) PollNode(pctx context.Context, nodeId string) {
	for {
		select {
		case <-pctx.Done():
			return
		case <-time.After(1 * time.Second):
			ctx, cancel := context.WithTimeout(pctx, 300*time.Millisecond)

			req := ReadRequest{
				Type: "read",
			}
			msg, _ := ls.n.SyncRPC(ctx, nodeId, req)
			var resp ReadResponse
			json.Unmarshal(msg.Body, &resp)

			ls.dbMu.Lock()

			for _, val := range resp.Messages {
				ls.Unlocked_ApplyWrite(val)
			}

			ls.dbMu.Unlock()
			cancel()
		}
	}
}

func (ls *LocalStore) PollNeighbors(ctx context.Context) {
	ls.l.Printf("Attempting to poll neighbors")
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
