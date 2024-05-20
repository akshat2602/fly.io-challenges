package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node
}

var logger = log.New(os.Stderr, "", 0)

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	return s.n.Reply(msg, body)
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "broadcast_ok"
	logger.Printf("Replying with ID: %d\n", body["id"])
	return s.n.Reply(msg, body)
}

func (s *server) HandleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "topology_ok"
	return s.n.Reply(msg, body)
}

func main() {
	var n = maelstrom.NewNode()

	s := &server{n: n}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.HandleTopology)

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
