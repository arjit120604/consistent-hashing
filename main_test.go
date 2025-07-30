package main

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

// TestNewRing tests the NewRing function.
func TestNewRing(t *testing.T) {
	ring := NewRing(10)

	if ring == nil {
		t.Fatal("NewRing returned a nil ring")
	}
	if ring.virtualReplicas != 10 {
		t.Errorf("Expected virtualReplicas to be 10, got %d", ring.virtualReplicas)
	}
	if len(ring.Nodes) != 0 {
		t.Errorf("Expected Nodes map to be empty, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 0 {
		t.Errorf("Expected virtualNodes slice to be empty, got %d", len(ring.virtualNodes))
	}
}

// TestAddNode tests the AddNode function.
func TestAddNode(t *testing.T) {
	ring := NewRing(3) // Use a small number of replicas for easier testing

	// Test adding a single node
	ring.AddNode("server1")
	if len(ring.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(ring.Nodes))
	}
	if _, ok := ring.Nodes["server1"]; !ok {
		t.Error("server1 not found in Nodes map")
	}
	if len(ring.virtualNodes) != 3 {
		t.Errorf("Expected 3 virtual nodes for server1, got %d", len(ring.virtualNodes))
	}

	// Verify virtual nodes are correctly associated and sorted
	foundVirtualNodes := make(map[string]int)
	for _, vn := range ring.virtualNodes {
		if vn.ID != "server1" {
			t.Errorf("Virtual node ID mismatch: expected server1, got %s", vn.ID)
		}
		foundVirtualNodes[vn.ID]++
	}
	if foundVirtualNodes["server1"] != 3 {
		t.Errorf("Expected 3 virtual nodes for server1, found %d", foundVirtualNodes["server1"])
	}

	// Check if virtual nodes are sorted by hash
	for i := 0; i < len(ring.virtualNodes)-1; i++ {
		if ring.virtualNodes[i].Hash > ring.virtualNodes[i+1].Hash {
			t.Errorf("Virtual nodes are not sorted by hash: %v", ring.virtualNodes)
			break
		}
	}

	// Test adding multiple nodes
	ring.AddNode("server2")
	ring.AddNode("server3")
	if len(ring.Nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 9 { // 3 nodes * 3 replicas
		t.Errorf("Expected 9 virtual nodes, got %d", len(ring.virtualNodes))
	}

	// Test adding an existing node (should not change anything)
	ring.AddNode("server1")
	if len(ring.Nodes) != 3 {
		t.Errorf("Expected 3 nodes after adding existing, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 9 {
		t.Errorf("Expected 9 virtual nodes after adding existing, got %d", len(ring.virtualNodes))
	}
}

// TestRemoveNode tests the RemoveNode function.
func TestRemoveNode(t *testing.T) {
	ring := NewRing(2)
	ring.AddNode("serverA")
	ring.AddNode("serverB")
	ring.AddNode("serverC")

	initialVirtualNodesCount := len(ring.virtualNodes)
	if initialVirtualNodesCount != 6 { // 3 nodes * 2 replicas
		t.Fatalf("Expected 6 initial virtual nodes, got %d", initialVirtualNodesCount)
	}

	// Test removing an existing node
	ring.RemoveNode("serverB")
	if len(ring.Nodes) != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", len(ring.Nodes))
	}
	if _, ok := ring.Nodes["serverB"]; ok {
		t.Error("serverB still found in Nodes map after removal")
	}
	if len(ring.virtualNodes) != 4 { // 2 nodes * 2 replicas
		t.Errorf("Expected 4 virtual nodes after removal, got %d", len(ring.virtualNodes))
	}
	for _, vn := range ring.virtualNodes {
		if vn.ID == "serverB" {
			t.Errorf("Found virtual node for serverB after removal")
		}
	}

	// Test removing a non-existent node (should not change anything)
	ring.RemoveNode("serverD")
	if len(ring.Nodes) != 2 {
		t.Errorf("Expected 2 nodes after removing non-existent, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 4 {
		t.Errorf("Expected 4 virtual nodes after removing non-existent, got %d", len(ring.virtualNodes))
	}

	// Remove all nodes
	ring.RemoveNode("serverA")
	ring.RemoveNode("serverC")
	if len(ring.Nodes) != 0 {
		t.Errorf("Expected 0 nodes after removing all, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 0 {
		t.Errorf("Expected 0 virtual nodes after removing all, got %d", len(ring.virtualNodes))
	}
}

// TestGetNode tests the GetNode function.
func TestGetNode(t *testing.T) {
	ring := NewRing(10)

	// Test with an empty ring
	node := ring.GetNode("some-key")
	if node != "" {
		t.Errorf("Expected empty string for empty ring, got %s", node)
	}

	// Add nodes
	ring.AddNode("server1")
	ring.AddNode("server2")
	ring.AddNode("server3")

	// Test with a single key
	key1 := "my-data-key"
	node1 := ring.GetNode(key1)
	if node1 == "" {
		t.Errorf("GetNode returned empty string for key %s", key1)
	}
	t.Logf("Key '%s' mapped to node '%s'", key1, node1)

	// Test consistency: same key should map to the same node
	node1Again := ring.GetNode(key1)
	if node1 != node1Again {
		t.Errorf("GetNode inconsistency: key '%s' mapped to '%s' then '%s'", key1, node1, node1Again)
	}

	// Test different keys
	key2 := "another-data-key"
	node2 := ring.GetNode(key2)
	if node2 == "" {
		t.Errorf("GetNode returned empty string for key %s", key2)
	}
	t.Logf("Key '%s' mapped to node '%s'", key2, node2)
	if node1 == node2 {
		t.Logf("Warning: Keys '%s' and '%s' mapped to the same node '%s'. This is possible but less ideal for distribution.", key1, key2, node1)
	}

	// Test wrap-around behavior (ensure it doesn't panic or return empty)
	// This is hard to test deterministically without knowing hash values,
	// but we can generate many keys and ensure they all map to *some* node.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("random-key-%d", i)
		n := ring.GetNode(key)
		if n == "" {
			t.Errorf("GetNode returned empty for random key %s", key)
		}
	}
}

// TestEndToEndScenario tests a full lifecycle of adding, getting, and removing nodes.
func TestEndToEndScenario(t *testing.T) {
	ring := NewRing(5) // 5 virtual replicas

	// 1. Add initial nodes
	ring.AddNode("serverA")
	ring.AddNode("serverB")
	ring.AddNode("serverC")

	if len(ring.Nodes) != 3 {
		t.Fatalf("Expected 3 nodes after initial add, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 15 { // 3 * 5
		t.Fatalf("Expected 15 virtual nodes after initial add, got %d", len(ring.virtualNodes))
	}

	// 2. Map some keys and record their initial nodes
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("data%d", i)
	}
	initialMappings := make(map[string]string)
	for _, key := range keys {
		initialMappings[key] = ring.GetNode(key)
		if initialMappings[key] == "" {
			t.Errorf("Key '%s' mapped to empty node initially", key)
		}
	}
	t.Logf("Initial mappings: %v", initialMappings)

	// 3. Remove a node
	t.Log("Removing serverB...")
	ring.RemoveNode("serverB")

	if len(ring.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes after removal, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 10 { // 2 * 5
		t.Fatalf("Expected 10 virtual nodes after removal, got %d", len(ring.virtualNodes))
	}
	if _, ok := ring.Nodes["serverB"]; ok {
		t.Error("serverB still present after removal")
	}

	// 4. Verify key re-distribution
	changedMappingsCount := 0
	for _, key := range keys {
		newNode := ring.GetNode(key)
		if newNode == "" {
			t.Errorf("Key '%s' mapped to empty node after removal", key)
		}
		if newNode == "serverB" {
			t.Errorf("Key '%s' still mapped to removed node 'serverB'", key)
		}
		if initialMappings[key] != newNode {
			changedMappingsCount++
		}
	}
	t.Logf("Mappings after removal: %v", func() map[string]string {
		m := make(map[string]string)
		for _, key := range keys {
			m[key] = ring.GetNode(key)
		}
		return m
	}())

	// Expect some keys to have changed their mapping, but not all (due to consistent hashing)
	// The exact number depends on hash distribution, but it should be > 0 and < len(keys)
	if changedMappingsCount == 0 {
		t.Error("No keys re-distributed after node removal, which is unexpected")
	}
	if changedMappingsCount == len(keys) {
		t.Log("All keys re-distributed after node removal. This is possible but might indicate less consistency than ideal.")
	}
	t.Logf("Keys re-distributed: %d/%d", changedMappingsCount, len(keys))

	// 5. Add a new node
	t.Log("Adding serverD...")
	ring.AddNode("serverD")
	if len(ring.Nodes) != 3 {
		t.Fatalf("Expected 3 nodes after adding new, got %d", len(ring.Nodes))
	}
	if len(ring.virtualNodes) != 15 { // 3 * 5
		t.Fatalf("Expected 15 virtual nodes after adding new, got %d", len(ring.virtualNodes))
	}

	// 6. Verify key re-distribution again
	changedMappingsCountAfterAdd := 0
	for _, key := range keys {
		finalNode := ring.GetNode(key)
		if finalNode == "" {
			t.Errorf("Key '%s' mapped to empty node after add", key)
		}
		if finalNode == initialMappings[key] {
			// If it mapped back to its original node, that's fine.
		} else if finalNode != initialMappings[key] {
			changedMappingsCountAfterAdd++
		}
	}
	t.Logf("Keys re-distributed after add: %d/%d", changedMappingsCountAfterAdd, len(keys))
	// Again, expect some changes, but not necessarily all.
	if changedMappingsCountAfterAdd == 0 {
		t.Error("No keys re-distributed after node addition, which is unexpected")
	}
}

// TestDistribution tests the distribution of keys across nodes.
func TestDistribution(t *testing.T) {
	numReplicas := 100 // More replicas for better distribution
	ring := NewRing(numReplicas)

	nodes := []string{"nodeA", "nodeB", "nodeC", "nodeD", "nodeE"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	numKeys := 10000
	nodeCounts := make(map[string]int)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		nodeCounts[node]++
	}

	t.Logf("Key distribution across nodes (total keys: %d):", numKeys)
	for node, count := range nodeCounts {
		t.Logf("  %s: %d (%.2f%%)", node, count, float64(count)/float64(numKeys)*100)
	}

	// Basic check for even distribution: each node should have a count within a reasonable range
	expectedPerNode := float64(numKeys) / float64(len(nodes))
	tolerance := 0.20 // 20% deviation from expected average

	for _, node := range nodes {
		count, ok := nodeCounts[node]
		if !ok {
			t.Errorf("Node %s received 0 keys, expected distribution", node)
			continue
		}
		deviation := (float64(count) - expectedPerNode) / expectedPerNode
		if deviation < -tolerance || deviation > tolerance {
			t.Errorf("Node %s distribution is uneven. Count: %d, Expected: %.2f, Deviation: %.2f%%",
				node, count, expectedPerNode, deviation*100)
		}
	}
}

// TestConcurrency tests concurrent access to the Ring.
func TestConcurrency(t *testing.T) {
	ring := NewRing(5)
	var wg sync.WaitGroup
	numGoroutines := 100
	numOperationsPerGoroutine := 100

	// Add nodes concurrently
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ring.AddNode(fmt.Sprintf("server-add-%d", i))
		}(i)
	}

	// Get nodes concurrently
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				ring.GetNode(fmt.Sprintf("key-%d-%d", i, j))
			}
		}(i)
	}
	wg.Wait()

	// Add more nodes and remove some concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("server-mix-%d", i)
			if i%2 == 0 {
				ring.AddNode(nodeID)
			} else {
				ring.RemoveNode(nodeID)
			}
			for j := 0; j < numOperationsPerGoroutine; j++ {
				ring.GetNode(fmt.Sprintf("key-mix-%d-%d", i, j))
			}
		}(i)
	}
	wg.Wait()

	// Final check: ensure no panics or deadlocks
	// This test primarily checks for race conditions by running many operations concurrently.
	// If it completes without panicking, it's a good sign.
	t.Log("Concurrent operations completed without panics or deadlocks.")
	t.Logf("Final number of nodes: %d", len(ring.Nodes))
	t.Logf("Final number of virtual nodes: %d", len(ring.virtualNodes))
}

// TestEdgeCases tests various edge cases.
func TestEdgeCases(t *testing.T) {
	// Test with 0 virtual replicas (should still work, but no virtual nodes)
	ring0 := NewRing(0)
	ring0.AddNode("server0")
	if len(ring0.Nodes) != 1 {
		t.Errorf("Expected 1 node with 0 replicas, got %d", len(ring0.Nodes))
	}
	if len(ring0.virtualNodes) != 0 {
		t.Errorf("Expected 0 virtual nodes with 0 replicas, got %d", len(ring0.virtualNodes))
	}
	if ring0.GetNode("key") != "" {
		t.Errorf("Expected empty node for 0 replicas, got %s", ring0.GetNode("key"))
	}

	// Test adding and removing many nodes sequentially
	ringMany := NewRing(1)
	numNodes := 1000
	for i := 0; i < numNodes; i++ {
		ringMany.AddNode(fmt.Sprintf("node-%d", i))
	}
	if len(ringMany.Nodes) != numNodes {
		t.Errorf("Expected %d nodes, got %d", numNodes, len(ringMany.Nodes))
	}
	if len(ringMany.virtualNodes) != numNodes {
		t.Errorf("Expected %d virtual nodes, got %d", numNodes, len(ringMany.virtualNodes))
	}

	for i := 0; i < numNodes; i++ {
		ringMany.RemoveNode(fmt.Sprintf("node-%d", i))
	}
	if len(ringMany.Nodes) != 0 {
		t.Errorf("Expected 0 nodes after removing all, got %d", len(ringMany.Nodes))
	}
	if len(ringMany.virtualNodes) != 0 {
		t.Errorf("Expected 0 virtual nodes after removing all, got %d", len(ringMany.virtualNodes))
	}

	// Test GetNode after all nodes are removed
	ringEmpty := NewRing(10)
	ringEmpty.AddNode("temp-node")
	ringEmpty.RemoveNode("temp-node")
	if ringEmpty.GetNode("some-key") != "" {
		t.Errorf("Expected empty string after all nodes removed, got %s", ringEmpty.GetNode("some-key"))
	}
}

// TestVirtualNodeSorting ensures virtual nodes are always sorted after additions.
func TestVirtualNodeSorting(t *testing.T) {
	ring := NewRing(5)
	nodesToAdd := []string{"serverZ", "serverA", "serverM", "serverP", "serverC"}

	for _, node := range nodesToAdd {
		ring.AddNode(node)
	}

	// Verify sorting after all additions
	if !sort.IsSorted(byHash(ring.virtualNodes)) {
		t.Errorf("Virtual nodes not sorted after all additions: %v", ring.virtualNodes)
	}
}

// Define a custom sort interface for virtualNode slice
// This is for internal testing verification, not part of the main logic.
type byHash []virtualNode

func (b byHash) Len() int { return len(b) }
func (b byHash) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byHash) Less(i, j int) bool { return b[i].Hash < b[j].Hash }