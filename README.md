# Consistent Hashing Implementation in Go

This repository contains a Go implementation of a consistent hashing ring. Consistent hashing is a technique that minimizes the number of keys that need to be remapped when a server is added or removed from a distributed cache.

## Implementation Details

The implementation consists of the following components:

*   **`Ring`**: The `Ring` struct represents the consistent hashing ring. It contains a map of nodes, a slice of virtual nodes, and the number of virtual replicas for each node.
*   **`virtualNode`**: The `virtualNode` struct represents a virtual node on the ring. It contains the ID of the physical node and the hash of the virtual node.
*   **`AddNode`**: The `AddNode` function adds a new node to the ring. It creates a number of virtual nodes for the new node and adds them to the ring. The virtual nodes are sorted by their hash value.
*   **`RemoveNode`**: The `RemoveNode` function removes a node from the ring. It removes all of the virtual nodes for the node from the ring.
*   **`GetNode`**: The `GetNode` function returns the node that is responsible for a given key. It hashes the key and then uses a binary search to find the first virtual node with a hash value that is greater than or equal to the hash of the key.

## Issues and Fixes

The initial implementation had two main issues:

1.  **Poor Key Distribution**: The `crc32` hash function did not provide a uniform distribution of keys and nodes on the ring. This caused some nodes to receive significantly more keys than others, which led to the `TestDistribution` test failing.

## Usage

To use the consistent hashing implementation, you can create a new `Ring` and then add and remove nodes as needed. To get the node for a given key, you can use the `GetNode` function.

```go
// Create a new ring with 10 virtual replicas per node
ring := NewRing(10)

// Add nodes to the ring
ring.AddNode("server1")
ring.AddNode("server2")
ring.AddNode("server3")

// Get the node for a given key
key := "my-key"
node := ring.GetNode(key)
```

## Testing

To run the tests, you can use the `go test` command:

```
go test
```
