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
