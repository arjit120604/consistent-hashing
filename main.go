package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// virtualNode
type virtualNode struct {
	ID   string
	Hash uint32
}

type Ring struct {
	Nodes          map[string]struct{}
	virtualNodes   []virtualNode
	virtualReplicas int
	mu             sync.RWMutex
}

// NewRing creates a new consistent hashing ring.
func NewRing(virtualReplicas int) *Ring {
	return &Ring{
		Nodes:          make(map[string]struct{}),
		virtualNodes:   []virtualNode{},
		virtualReplicas: virtualReplicas,
	}
}

// AddNode adds a new node to the ring.
func (r *Ring) AddNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()//ensures the lock is released after the funciton returns

	//check if the node already exists
	if _, ok := r.Nodes[id]; ok {
		return
	}

	r.Nodes[id] = struct{}{}
	for i := 0; i < r.virtualReplicas; i++ {
		virtualID := id + "-" + strconv.Itoa(i)
		hashBytes := sha256.Sum256([]byte(virtualID))
		hash := binary.BigEndian.Uint32(hashBytes[:4])//convert the first 4 bytes of the hash to uint32
		r.virtualNodes = append(r.virtualNodes, virtualNode{ID: id, Hash: hash})
	}
	sort.Slice(r.virtualNodes, func(i, j int) bool {
		return r.virtualNodes[i].Hash < r.virtualNodes[j].Hash
	})
}

// RemoveNode removes a node from the ring.
func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.Nodes[id]; !ok {
		return // Node does not exist
	}

	delete(r.Nodes, id)

	kept := 0
	for i := range r.virtualNodes {
		if r.virtualNodes[i].ID != id {
			r.virtualNodes[kept] = r.virtualNodes[i]
			kept++
		}
	}
	r.virtualNodes = r.virtualNodes[:kept]//only keep the kept no of virtual nodes
}

// GetNode returns the node responsible for the given key.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.virtualNodes) == 0 {
		return ""
	}

	hashBytes := sha256.Sum256([]byte(key))
	hash := binary.BigEndian.Uint32(hashBytes[:4])
	i := sort.Search(len(r.virtualNodes), func(i int) bool {
		return r.virtualNodes[i].Hash >= hash
	})

	if i == len(r.virtualNodes) {
		i = 0
	}
	return r.virtualNodes[i].ID
}

func main() {
	ring := NewRing(100)
	fmt.Println("Consistent Hashing Interactive CLI")
	fmt.Println("Commands: add <node>, remove <node>, get <key>, nodes, exit")

	// Add some initial nodes
	ring.AddNode("server1")
	ring.AddNode("server2")
	ring.AddNode("server3")
	fmt.Println("Initial nodes: server1, server2, server3")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		args := parts[1:]

		switch command {
		case "add":
			if len(args) != 1 {
				fmt.Println("Usage: add <node_id>")
				continue
			}
			nodeID := args[0]
			ring.AddNode(nodeID)
			fmt.Printf("Added node '%s'\n", nodeID)
		case "remove":
			if len(args) != 1 {
				fmt.Println("Usage: remove <node_id>")
				continue
			}
			nodeID := args[0]
			if _, ok := ring.Nodes[nodeID]; !ok {
				fmt.Printf("Node '%s' does not exist\n", nodeID)
				continue
			}
			ring.RemoveNode(nodeID)
			fmt.Printf("Removed node '%s'\n", nodeID)
		case "get":
			if len(args) != 1 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := args[0]
			node := ring.GetNode(key)
			if node == "" {
				fmt.Println("No nodes in the ring.")
			} else {
				fmt.Printf("Key '%s' is mapped to node '%s'\n", key, node)
			}
		case "nodes":
			ring.mu.RLock()
			nodes := make([]string, 0, len(ring.Nodes))
			for nodeID := range ring.Nodes {
				nodes = append(nodes, nodeID)
			}
			ring.mu.RUnlock()
			sort.Strings(nodes) // Sort for consistent output
			if len(nodes) == 0 {
				fmt.Println("No nodes in the ring.")
			} else {
				fmt.Println("Current nodes:", strings.Join(nodes, ", "))
			}
		case "exit":
			fmt.Println("Exiting.")
			return
		default:
			fmt.Println("Unknown command. Available: add, remove, get, nodes, exit")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}