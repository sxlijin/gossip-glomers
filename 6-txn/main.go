package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalStore struct {
	n *maelstrom.Node
	l *log.Logger

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
	ls.n.Handle("Txn", ls.HandleTxn)

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
		l:  log.Default(),
		db: make(map[string][]int),
	}

	i := 5
	ls.l.Printf("%d", i)

	msg := `{
  "type": "txn",
  "msg_id": 3,
  "txn": [
    ["r", 1, null],
    ["w", 1, 6],
    ["w", 2, 9]
  ]
}`
	var req TxnRequest
	if err := json.Unmarshal([]byte(msg), &req); err != nil {
		ls.l.Printf("unmarshal err: %s", err)
	}
	ls.l.Printf("unmarshal -> %#v", req)

	// if err := ls.Run(); err != nil {
	// 	log.Fatal(err)
	// }
}
