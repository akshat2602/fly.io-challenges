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
	pendingBroadcasts chan broadcastMsg
	messages          map[float64]struct{}
	// A map to store the broadcast data for each node
	broadcastData map[string][]float64
	// A mutex to lock the messages map
	messageLock       sync.RWMutex
	broadcastDataLock sync.RWMutex
	topologyLock      sync.RWMutex
}

type broadcastMsg struct {
	dst  string
	body map[string]any
	src  string
}

var logger = log.New(os.Stderr, "", 0)

func broadcastWorkers(s *server) {
	maxRetries := 100
	s.broadcastDataLock.Lock()
	wg := sync.WaitGroup{}
	for dst, data := range s.broadcastData {
		if len(data) == 0 {
			continue
		}
		wg.Add(1)
		go func(data []float64, dst string) {
			attempts := 0
			body := map[string]any{
				"type":     "broadcast",
				"messages": data,
			}
			for {
				logger.Printf("Sending broadcast message %v to: %s", body, dst)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := s.node.SyncRPC(ctx, dst, body)
				cancel()
				logger.Printf("err: %v", err)
				if err != nil {
					attempts++
					if attempts < maxRetries {
						time.Sleep(time.Duration(attempts) * time.Second)
						continue
					} else {
						logger.Printf("Max retries reached for message %v to: %s", body, dst)
						wg.Done()
						break
					}
				}
				wg.Done()
				break
			}
		}(data, dst)
	}
	wg.Wait()
	s.broadcastData = map[string][]float64{}
	s.broadcastDataLock.Unlock()
}

func (s *server) broadcastValues() {
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
		bMessage := <-s.pendingBroadcasts

		for _, address := range neighbors {
			if bMessage.src == address {
				continue
			}
			bMessage.dst = address
			s.broadcastDataLock.Lock()
			logger.Printf("Adding message %v to broadcast data for node: %s", float64(bMessage.body["message"].(int)), address)
			if _, ok := s.broadcastData[address]; ok {
				s.broadcastData[address] = append(s.broadcastData[address], float64(bMessage.body["message"].(int)))
			} else {
				s.broadcastData[address] = []float64{}
				s.broadcastData[address] = append(s.broadcastData[address], float64(bMessage.body["message"].(int)))
			}
			logger.Printf("Broadcast data for node %s: %v", address, s.broadcastData[address])
			s.broadcastDataLock.Unlock()
			// b.broadcastChan <- bMessage
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
	for id := range s.messages {
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

	if _, exists := body["messages"]; !exists {
		s.messageLock.Lock()
		if _, exists := s.messages[body["message"].(float64)]; exists {
			logger.Printf("Message already exists in local state")
			s.messageLock.Unlock()
			return nil
		}

		s.messages[body["message"].(float64)] = struct{}{}
		s.messageLock.Unlock()

		s.pendingBroadcasts <- broadcastMsg{
			body: map[string]any{
				"type":    "broadcast",
				"message": int(body["message"].(float64)),
			},
			src: msg.Src,
		}
	} else {
		for _, v := range body["messages"].([]interface{}) {
			s.messageLock.Lock()
			if _, exists := s.messages[v.(float64)]; exists {
				logger.Printf("Message already exists in local state")
				s.messageLock.Unlock()
				continue
			}
			s.messages[v.(float64)] = struct{}{}
			s.messageLock.Unlock()

			s.pendingBroadcasts <- broadcastMsg{
				body: map[string]any{
					"type":    "broadcast",
					"message": int(v.(float64)),
				},
				src: msg.Src,
			}
		}
	}

	logger.Printf("Added message to local state")
	return nil
}

func (s *server) handleTopology(msg maelstrom.Message) error {
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

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func main() {
	var n = maelstrom.NewNode()
	s := &server{
		node:              n,
		topology:          map[string][]string{},
		pendingBroadcasts: make(chan broadcastMsg, 2500),
		messages:          map[float64]struct{}{},
		broadcastData:     map[string][]float64{},
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	go s.broadcastValues()

	go func() {
		for {
			<-time.After(time.Duration(500) * time.Millisecond)
			broadcastWorkers(s)
		}
	}()

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
