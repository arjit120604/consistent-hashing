package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
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
	defer r.mu.Unlock()

	if _, ok := r.Nodes[id]; ok {
		return // Node already exists
	}

	r.Nodes[id] = struct{}{}
	for i := 0; i < r.virtualReplicas; i++ {
		virtualID := id + "-" + strconv.Itoa(i)
		hashBytes := sha256.Sum256([]byte(virtualID))
		hash := binary.BigEndian.Uint32(hashBytes[:4])
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
	var newVirtualNodes []virtualNode
	for _, vNode := range r.virtualNodes {
		if vNode.ID != id {
			newVirtualNodes = append(newVirtualNodes, vNode)
		}
	}
	r.virtualNodes = newVirtualNodes
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
	ring := NewRing(10)

	ring.AddNode("server1")
	ring.AddNode("server2")
	ring.AddNode("server3")

	key1 := "my-key"
	node1 := ring.GetNode(key1)
	fmt.Printf("Key '%s' is mapped to node '%s'\n", key1, node1)

	key2 := "another-key"
	node2 := ring.GetNode(key2)
	fmt.Printf("Key '%s' is mapped to node '%s'\n", key2, node2)

	fmt.Println("\nRemoving node 'server2'...")
	ring.RemoveNode("server2")

	node1_after_removal := ring.GetNode(key1)
	fmt.Printf("Key '%s' is now mapped to node '%s'\n", key1, node1_after_removal)

	node2_after_removal := ring.GetNode(key2)
	fmt.Printf("Key '%s' is now mapped to node '%s'\n", key2, node2_after_removal)
}