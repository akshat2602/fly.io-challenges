package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node              *maelstrom.Node
	topology          map[string][]string
	pendingBroadcasts chan int
	broadcastData     map[float64]struct{}
	// A mutex to lock the broadcastData map
	messageLock sync.Mutex
}

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type topologyMessage struct {
	Topology map[string][]string `json:"topology"`
}

// Nitpick: Currently using a global logger, should be passed to the server struct

var logger = log.New(os.Stderr, "", 0)

func (s *server) broadcastValues() {
	for {
		nodeID := s.node.ID()
		neighbors := s.topology[nodeID]
		if len(neighbors) == 0 {
			// logger.Printf("No neighbors found for node: %s", nodeID)
			continue
		}
		logger.Printf("Waiting for pending broadcasts")
		message := <-s.pendingBroadcasts

		logger.Printf("Broadcasting message: %d to neighbors: %v", message, neighbors)

		msg := broadcastMessage{
			Type:    "broadcast",
			Message: message,
		}
		// Convert the message to bytes
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			logger.Printf("Error marshalling message: %v", err)
			continue
		}
		// Create a raw message
		bmsg := json.RawMessage(msgBytes)
		for _, address := range neighbors {
			logger.Printf("Broadcasting message to %s: %v", address, msg)
			s.node.RPC(address, bmsg, func(msg maelstrom.Message) error {
				var body map[string]interface{}
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				if body["type"] != "broadcast_ok" {
					return errors.New("unexpected response type")
				}
				logger.Printf("Broadcast response received: %v", body)
				return nil
			})
		}
	}
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	logger.Printf("Read request received: %v", body)

	ids := []float64{}
	s.messageLock.Lock()
	defer s.messageLock.Unlock()
	for id := range s.broadcastData {
		ids = append(ids, id)
	}

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "messages": ids})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	logger.Printf("Broadcast request received: %v", body)
	s.messageLock.Lock()
	defer s.messageLock.Unlock()
	if _, exists := s.broadcastData[body["message"].(float64)]; exists {
		logger.Printf("Message already exists in local state")
		return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}
	s.broadcastData[body["message"].(float64)] = struct{}{}
	s.pendingBroadcasts <- int(body["message"].(float64))

	logger.Printf("Added message to local state")
	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var t topologyMessage
	if err := json.Unmarshal(msg.Body, &t); err != nil {
		return err
	}

	s.topology = t.Topology

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	var n = maelstrom.NewNode()
	s := &server{
		node:              n,
		topology:          map[string][]string{},
		pendingBroadcasts: make(chan int, 10),
		broadcastData:     map[float64]struct{}{},
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	go s.broadcastValues()

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
