package consensusnode

import (
	"errors"
	"math"
	"math/rand"
	"time"
	// "fmt" // Uncomment if needed for debugging within this file.
	// "log" // Uncomment if needed for logging within this file.
)

// NodeID defines the type for a node identifier.
// In the context of Lachesis, this is typically the hex string of the node's public key (creator).
type NodeID string

// SelectReferenceNode implements Algorithm 2: Node Selection from the Lachesis protocol.
// This algorithm selects a reference node from a set of candidate nodes based on cost.
//
// Parameters:
//   - heights: A map representing the Height Vector (H).
//     Keys are NodeIDs (hex public key strings), and values are their corresponding heights
//     (e.g., the index of the latest event from that node).
//   - inDegrees: A map representing the In-degree Vector (I).
//     Keys are NodeIDs, and values are their corresponding in-degrees
//     (e.g., the number of OtherParents from distinct creators in the node's latest event).
//   - candidateNodes: A slice of NodeIDs representing the set of candidate nodes
//     (excluding the current node performing the selection).
//
// Returns:
//   - The NodeID of the selected reference node.
//   - An error if no suitable node can be selected (e.g., if candidateNodes is empty).
func SelectReferenceNode(heights map[NodeID]uint64, inDegrees map[NodeID]uint64, candidateNodes []NodeID) (NodeID, error) {
	if len(candidateNodes) == 0 {
		return "", errors.New("candidateNodes cannot be empty")
	}

	minCost := math.MaxFloat64
	var sref []NodeID // Slice to store nodes with the lowest cost.

	for _, k := range candidateNodes {
		hk, hExists := heights[k]
		ik, iExists := inDegrees[k]

		var currentCost float64
		if !hExists || !iExists || hk == 0 {
			// If node k is not in heights or inDegrees, or its height (hk) is 0,
			// its cost is considered infinite.
			currentCost = math.MaxFloat64

			// Handle the case where all nodes initially have infinite cost (e.g., all hk=0).
			// Add to sref only if ik exists (node has some basic data) and hk is 0.
			// This ensures we don't add nodes with no data at all.
			if minCost == math.MaxFloat64 && hk == 0 && iExists {
				// Check for duplicates before adding.
				isAlreadyInSref := false
				for _, existingNode := range sref {
					if existingNode == k {
						isAlreadyInSref = true
						break
					}
				}
				if !isAlreadyInSref {
					sref = append(sref, k)
				}
			}
		} else {
			currentCost = float64(ik) / float64(hk)
		}

		if currentCost < minCost {
			minCost = currentCost
			sref = []NodeID{k} // Reset sref with only node k.
		} else if currentCost == minCost {
			// If costs are equal, add node k to sref.
			// Ensure not to add nodes with infinite cost unless minCost is also infinite.
			if currentCost != math.MaxFloat64 || minCost == math.MaxFloat64 {
				// Check for duplicates to avoid adding the same node multiple times.
				isAlreadyInSref := false
				for _, existingNode := range sref {
					if existingNode == k {
						isAlreadyInSref = true
						break
					}
				}
				if !isAlreadyInSref {
					sref = append(sref, k)
				}
			}
		}
	}

	if len(sref) == 0 {
		// This can happen if candidateNodes is not empty, but all candidates
		// had invalid data (e.g., all hk=0 and no valid ik).
		return "", errors.New("no suitable reference node found (sref is empty after processing candidates)")
	}

	// Randomly select a node from sref (nodes with the minimum cost).
	// For production environments, consider a more robust random source
	// or passing an initialized rand.Rand instance.
	//nolint:gosec // Using math/rand for non-cryptographic random selection is acceptable here.
	source := rand.NewSource(time.Now().UnixNano())
	randomGenerator := rand.New(source)

	randomIndex := randomGenerator.Intn(len(sref))
	selectedRefNodeID := sref[randomIndex]

	return selectedRefNodeID, nil
}

// GetNodeIDFromString converts a string to a NodeID type.
// This is a simple helper but can be useful for type clarity when working with public key strings.
func GetNodeIDFromString(s string) NodeID {
	return NodeID(s)
}
