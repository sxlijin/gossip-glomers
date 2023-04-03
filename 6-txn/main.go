package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalStore struct {
	n *maelstrom.Node
	l *log.Logger

	init sync.WaitGroup

	db   map[int]int
	dbMu sync.Mutex
}

type OpType string

const (
	WriteOp = "w"
	ReadOp  = "r"
)

type Op struct {
	Type  OpType
	Key   int
	Value int
}

func (ls *LocalStore) Unlocked_ApplyOps(ops []Op) {
	for i, op := range ops {
		switch op.Type {
		case WriteOp:
			ls.db[op.Key] = op.Value
		case ReadOp:
			ops[i].Value = ls.db[op.Key]
		default:
			ls.l.Printf("Unrecognized op, cannot apply")
		}
	}
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
	ls.n.Handle("txn", ls.HandleTxn)

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
		db: make(map[int]int),
	}

	if err := ls.Run(); err != nil {
		log.Fatal(err)
	}
}
