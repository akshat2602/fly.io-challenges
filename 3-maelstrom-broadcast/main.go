package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastData struct {
	message []float64
}

type server struct {
	node              *maelstrom.Node
	topology          map[string][]string
	pendingBroadcasts chan int
	broadcastData     broadcastData
}

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

// Nitpick: Currently using a global logger, should be passed to the server struct
// TODO: Implement atleast once semantics, right now all values are stored as is and marked for rebroadcast

var logger = log.New(os.Stderr, "", 0)

func (s *server) broadcastValues() {
	for {
		nodeID := s.node.ID()
		neighbors := s.topology[nodeID]
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
		// Clear the pending broadcasts
		// TODO: Check if a lock is needed here
		s.pendingBroadcasts = make(chan int, 10)
	}

}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	logger.Printf("Read request received: %v", body)
	body["messages"] = s.broadcastData.message
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
	s.broadcastData.message = append(s.broadcastData.message, body["message"].(float64))
	s.pendingBroadcasts <- int(body["message"].(float64))
	logger.Printf("Added message to local state: %v", s.broadcastData.message)
	// delete the key message from the body
	delete(body, "message")
	return s.node.Reply(msg, body)
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "topology_ok"
	// body["topology"] is of type map[string]interface{} but we need to convert it to map[string][]string
	// to be able to use it in the broadcastValues function
	topology := map[string][]string{}
	for k, v := range body["topology"].(map[string]interface{}) {
		neighbors := []string{}
		for _, neighbor := range v.([]interface{}) {
			neighbors = append(neighbors, neighbor.(string))
		}
		topology[k] = neighbors
	}
	s.topology = topology

	// delete the key topology from the body
	delete(body, "topology")
	return s.node.Reply(msg, body)
}

func main() {
	var n = maelstrom.NewNode()
	s := &server{
		node:              n,
		topology:          map[string][]string{},
		pendingBroadcasts: make(chan int, 10),
		broadcastData:     broadcastData{message: []float64{}},
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	go s.broadcastValues()

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
