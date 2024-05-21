package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node     *maelstrom.Node
	topology map[string][]string
}

type broadcastData struct {
	message []float64
}

var b = broadcastData{message: []float64{}}

var logger = log.New(os.Stderr, "", 0)

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	logger.Printf("Read request received: %v", body)
	body["messages"] = b.message
	logger.Printf("Read response: %v", body)
	return s.node.Reply(msg, body)
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	logger.Printf("Broadcast request received: %v", body)
	body["type"] = "broadcast_ok"
	b.message = append(b.message, body["message"].(float64))
	logger.Printf("Added message to local state: %v", b.message)
	// delete the key message from the body
	delete(body, "message")
	return s.node.Reply(msg, body)
}

func (s *server) HandleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "topology_ok"
	// delete the key topology from the body
	delete(body, "topology")
	return s.node.Reply(msg, body)
}

func main() {
	var n = maelstrom.NewNode()
	s := &server{node: n, topology: map[string][]string{}}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.HandleTopology)

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
