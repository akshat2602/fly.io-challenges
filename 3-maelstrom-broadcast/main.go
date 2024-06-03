package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	node              *maelstrom.Node
	topology          map[string][]string
	pendingBroadcasts chan int
	broadcastData     map[float64]struct{}
	// A mutex to lock the broadcastData map
	messageLock  sync.RWMutex
	topologyLock sync.RWMutex
}

type broadcaster struct {
	broadcastChan chan broadcastMsg
}

type broadcastMsg struct {
	dst  string
	body map[string]any
}

type topologyMessage struct {
	Topology map[string][]string `json:"topology"`
}

var logger = log.New(os.Stderr, "", 0)

func (b *broadcaster) bWorkers(node *maelstrom.Node) {
	maxRetries := 100
	for i := 0; i < 75; i++ {
		go func() {
			for {
				attempts := 0
				bmsg := <-b.broadcastChan
				for {
					logger.Printf("Sending broadcast message %v to: %s", bmsg.body, bmsg.dst)
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(600))
					_, err := node.SyncRPC(ctx, bmsg.dst, bmsg.body)
					cancel()
					logger.Printf("err: %v", err)
					if err != nil {
						attempts++
						if attempts < maxRetries {
							time.Sleep(time.Duration(attempts) * time.Millisecond * time.Duration(400))
							continue
						} else {
							logger.Printf("Max retries reached for message %v to: %s", bmsg.body, bmsg.dst)
							break
						}
					}
					break
				}
			}
		}()
	}
}

func (s *server) broadcastValues(b *broadcaster) {
	for {
		nodeID := s.node.ID()
		s.topologyLock.RLock()
		neighbors := s.topology[nodeID]
		s.topologyLock.RUnlock()
		if len(neighbors) == 0 {
			// logger.Printf("No neighbors found for node: %s", nodeID)
			continue
		}
		logger.Printf("Waiting for pending broadcasts")
		message := <-s.pendingBroadcasts

		for _, address := range neighbors {
			b.broadcastChan <- broadcastMsg{
				dst: address,
				body: map[string]any{
					"type":    "broadcast",
					"message": message,
				},
			}
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
	s.messageLock.RLock()
	for id := range s.broadcastData {
		ids = append(ids, id)
	}
	s.messageLock.RUnlock()

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "messages": ids})
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go func() {
		_ = s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}()

	logger.Printf("Broadcast request received: %v", body)
	s.messageLock.Lock()
	if _, exists := s.broadcastData[body["message"].(float64)]; exists {
		logger.Printf("Message already exists in local state")
		s.messageLock.Unlock()
		return nil
	}
	s.broadcastData[body["message"].(float64)] = struct{}{}
	s.messageLock.Unlock()
	s.pendingBroadcasts <- int(body["message"].(float64))

	logger.Printf("Added message to local state")
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var t topologyMessage
	if err := json.Unmarshal(msg.Body, &t); err != nil {
		return err
	}

	idx := slices.IndexFunc(s.node.NodeIDs(), func(s string) bool { return s == "n0" })

	s.topologyLock.Lock()
	s.topology = map[string][]string{
		"n0": append(s.node.NodeIDs()[:idx], s.node.NodeIDs()[idx+1:]...), // remove n0 from the list
	}
	for _, v := range s.node.NodeIDs() {
		if v != "n0" {
			s.topology[v] = []string{"n0"}
		}
	}
	s.topologyLock.Unlock()

	logger.Printf("Topology received: %v", s.topology)
	// s.topology = t.Topology

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	var n = maelstrom.NewNode()
	s := &server{
		node:              n,
		topology:          map[string][]string{},
		pendingBroadcasts: make(chan int, 400),
		broadcastData:     map[float64]struct{}{},
	}
	b := &broadcaster{
		broadcastChan: make(chan broadcastMsg, 400),
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	go s.broadcastValues(b)
	b.bWorkers(n)

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
