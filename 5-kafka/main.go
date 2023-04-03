package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalStore struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
	l  *log.Logger

	init sync.WaitGroup

	db   map[string][]int
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

	if err := ls.n.Run(); err != nil {
		ls.l.Fatal(err)
		return err
	}

	return nil
}

func main() {
	n := maelstrom.NewNode()
	ls := LocalStore{
		n:  n,
		kv: maelstrom.NewSeqKV(n),
		l:  log.Default(),
		db: make(map[string][]int),
	}

	if err := ls.Run(); err != nil {
		log.Fatal(err)
	}
}
