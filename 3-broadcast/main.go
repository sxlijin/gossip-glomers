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

type response2 struct {
	Page   int      `json:"page"`
	Fruits []string `json:"fruits"`
}

func main() {
	n := maelstrom.NewNode()
	l := log.Default()

	var av AtomicVal
	av.val = make([]int64, 0)

	n.Handle("broadcast", func(req maelstrom.Message) error {
		var reqBody struct {
			MsgId   string `json:"msg_id"`
			Type    string `json:"type"`
			Message int64  `json:"message"`
		}
		if err := json.Unmarshal(req.Body, &reqBody); err != nil {
			return err
		}
		respBody := make(map[string]any)

		av.mu.Lock()
		defer av.mu.Unlock()

		av.val = append(av.val, reqBody.Message)
		respBody["msg_id"] = reqBody.MsgId
		respBody["type"] = "broadcast_ok"

		return n.Reply(req, respBody)
	})

	n.Handle("read", func(req maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(req.Body, &reqBody); err != nil {
			return err
		}
		respBody := make(map[string]any)

		av.mu.Lock()
		defer av.mu.Unlock()

		c := make([]int64, len(av.val))
		copy(c, av.val)
		respBody["type"] = "read_ok"
		respBody["msg_id"] = reqBody["msg_id"]
		respBody["messages"] = c

		return n.Reply(req, respBody)
	})

	n.Handle("topology", func(req maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(req.Body, &reqBody); err != nil {
			return err
		}
		respBody := make(map[string]any)

		respBody["type"] = "topology_ok"
		respBody["msg_id"] = reqBody["msg_id"]

		return n.Reply(req, respBody)
	})

	l.Printf("program started")

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
