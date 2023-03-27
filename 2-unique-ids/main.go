package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AtomicInt struct {
	val int64
	mu  sync.Mutex
}

func main() {
	n := maelstrom.NewNode()
	l := log.Default()

	var id AtomicInt

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id.mu.Lock()
		defer id.mu.Unlock()

		l.Printf("Received maelstrom message %#v", msg)
		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%s-%d", msg.Dest, id.val)
		id.val += 1

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
