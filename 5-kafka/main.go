package main

import (
	"context"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalStore struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
	l  *log.Logger

	init sync.WaitGroup

	db   map[string]int
	dbMu sync.Mutex
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
	ls.n.Handle("send", ls.HandleSend)
	ls.n.Handle("poll", ls.HandlePoll)
	ls.n.Handle("commit_offsets", ls.HandleCommitOffsets)
	ls.n.Handle("list_committed_offsets", ls.HandleListCommittedOffsets)

	ctx, cancel := context.WithCancel(context.Background())
	go ls.ManagePollers(ctx)

	if err := ls.n.Run(); err != nil {
		ls.l.Fatal(err)
		return err
	}

	cancel()

	return nil
}

func (ls *LocalStore) Unlocked_ApplyDelta(nodeId string, delta int) {
	ls.db[nodeId] += delta
}

func (ls *LocalStore) ManagePollers(ctx context.Context) {
	ls.init.Wait()

	pollers := make(map[string]struct{})
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
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
			if val, err := ls.kv.ReadInt(ctx, nodeId); err == nil {
				ls.db[nodeId] = val
			} else {
				ls.l.Printf("Error occurred while reading from the KV store: %s", err)
			}
			cancel()
		}
	}
}

func (ls *LocalStore) PollNeighbors(ctx context.Context) {
	ls.l.Printf("Attempting to poll neighbors")
}

func main() {
	n := maelstrom.NewNode()
	ls := LocalStore{
		n:  n,
		kv: maelstrom.NewSeqKV(n),
		l:  log.Default(),
		db: make(map[string]int),
	}

	if err := ls.Run(); err != nil {
		log.Fatal(err)
	}
}
