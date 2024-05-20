package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node
}

var logger = log.New(os.Stderr, "", 0)

func (s *server) generateUniqueID() int64 {
	nodeID := []byte(s.n.ID())[0]

	// My original solution was using the node ID and the time as seed but I feel like it might not be that cryptographically secure
	// Using crypto/rand might be a good option similar to this https://github.com/teivah/gossip-glomers/blob/main/challenge-2-unique-id/main.go
	// I'm not sure if this is the best way to do it but I think it's a good start
	random := rand.New(rand.NewSource(time.Now().UnixNano() + int64(nodeID)))
	r := random.Int63()
	logger.Printf("Generated ID: %d\n", r)
	return r
}

func (s *server) handleGenerateID(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = s.generateUniqueID()
	logger.Printf("Replying with ID: %d\n", body["id"])
	return s.n.Reply(msg, body)
}

func main() {
	var n = maelstrom.NewNode()

	s := &server{n: n}

	n.Handle("generate", s.handleGenerateID)

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}

}
