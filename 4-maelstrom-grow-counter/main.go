package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
	ctx  context.Context
}

var logger = log.New(os.Stderr, "", 0)

func (s *server) handleAdd(msg maelstrom.Message) error {
	// Add the value to the counter
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	val := int(body["delta"].(float64))

	rVal, err := s.kv.ReadInt(s.ctx, "counter")
	if rerr, ok := err.(*maelstrom.RPCError); ok && rerr.Code == maelstrom.KeyDoesNotExist {
		rVal = 0
		s.kv.Write(s.ctx, "counter", 0)
	} else if err != nil {
		logger.Printf("Error reading counter: %v", err)
	}

	err = s.kv.CompareAndSwap(s.ctx, "counter", rVal, rVal+val, true)
	if err != nil {
		if rerr, ok := err.(*maelstrom.RPCError); ok && rerr.Code == maelstrom.PreconditionFailed {
			logger.Printf("CAS failed: %v", err)
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	// Read the value of the counter
	val := 0
	val, err := s.kv.ReadInt(s.ctx, "counter")
	if err != nil {
		logger.Printf("Error reading counter: %v", err)
	}

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "value": val})
}

func main() {
	var n = maelstrom.NewNode()
	globalCtx := context.Context(context.Background())
	s := &server{node: n, ctx: globalCtx}

	kv := maelstrom.NewSeqKV(s.node)
	s.kv = kv

	s.node.Handle("add", s.handleAdd)
	s.node.Handle("read", s.handleRead)

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
