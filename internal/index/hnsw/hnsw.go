package hnsw

import (
	"errors"
	"fmt"
	"math"
	"math/rand"

	"github.com/msr23/veclite/internal/index/types"
	"github.com/msr23/veclite/internal/index/utils"
	"github.com/msr23/veclite/internal/storage"
	"github.com/msr23/veclite/internal/vector"
)

// HNSWNode represents a node in the HNSW graph
// Memory-efficient: only stores ID and neighbor connections
// Vectors are NOT stored in memory - retrieved from storage when needed
type HNSWNode struct {
	ID        uint64     // Vector ID
	Level     int        // Maximum level this node appears in (0 = bottom layer)
	Neighbors [][]uint64 // Neighbors[level] = neighbor IDs at that level
}

// candidate represents a potential nearest neighbor during search or insert
// This is a local type for searchLevel return value
type candidate struct {
	id       uint64  // Vector/node ID
	distance float32 // Distance to query vector
}

// HNSWIndex implements Hierarchical Navigable Small World index
// Memory-efficient: only stores graph structure (IDs and connections)
type HNSWIndex struct {
	dimension int
	config    map[string]any
	storage   *storage.Storage // Storage for vectors (vectors NOT in memory)

	// Graph structure (memory-efficient: only IDs and connections)
	nodes      map[uint64]*HNSWNode // All nodes by ID
	entryPoint uint64               // Top-level entry point ID
	maxLevel   int                  // Highest layer level
	size       int                  // Number of vectors

	// HNSW parameters
	M              int     // Maximum number of connections per node
	efConstruction int     // Search width during construction
	efSearch       int     // Search width during query
	mL             float64 // Level generation parameter (typically 1/ln(2))
	// NOTE: Cache is now handled by storage layer
}

// NewHNSWIndex creates a new HNSW index
// storage is required for HNSW to store vectors on disk
func NewHNSWIndex(dimension int, config map[string]any, storage *storage.Storage) (*HNSWIndex, error) {
	// Extract HNSW parameters from config
	M := 16
	if m, ok := config["M"].(int); ok {
		M = m
	}

	efConstruction := 200
	if ef, ok := config["EfConstruction"].(int); ok {
		efConstruction = ef
	}

	efSearch := 50
	if ef, ok := config["EfSearch"].(int); ok {
		efSearch = ef
	}

	// mL is typically 1/ln(2) ≈ 1.44
	mL := 1.0 / math.Log(2.0)

	return &HNSWIndex{
		dimension:      dimension,
		config:         config,
		storage:        storage,
		nodes:          make(map[uint64]*HNSWNode),
		entryPoint:     0, // Will be set on first insert
		maxLevel:       -1,
		size:           0,
		M:              M,
		efConstruction: efConstruction,
		efSearch:       efSearch,
		mL:             mL,
	}, nil
}

// Insert adds a vector to the HNSW index
// Algorithm:
// 1. Write vector to storage
// 2. Generate random level for new node (exponential distribution)
// 3. If first node, set as entry point
// 4. Search for neighbors at each level from top to bottom
// 5. At each level, select M best neighbors from efConstruction candidates
// 6. Connect new node to selected neighbors
// 7. Update neighbors' connections (prune to maintain max M connections)
// 8. Update entry point if new node is at higher level
func (h *HNSWIndex) Insert(id uint64, vec []float32) error {
	if len(vec) != h.dimension {
		return types.ErrDimensionMismatch
	}

	// Check if node already exists
	if _, exists := h.nodes[id]; exists {
		// Node exists, update the vector in storage
		if h.storage != nil {
			if err := h.storage.WriteVector(id, vec); err != nil {
				return fmt.Errorf("failed to update vector in storage: %w", err)
			}
		}
		return nil
	}

	// Step 1: Write vector to storage
	if h.storage != nil {
		if err := h.storage.WriteVector(id, vec); err != nil {
			return fmt.Errorf("failed to write vector to storage: %w", err)
		}
	}

	// Step 2: Generate random level using exponential distribution
	// Level = floor(-ln(U) / mL) where U is uniform random in (0,1)
	u := rand.Float64()
	if u <= 0 {
		u = 0.0001 // Avoid log(0)
	}
	level := int(math.Floor(-math.Log(u) / h.mL))
	if level < 0 {
		level = 0
	}

	// Step 3: If this is the first node, set as entry point
	if h.entryPoint == 0 || len(h.nodes) == 0 {
		node := &HNSWNode{
			ID:        id,
			Level:     level,
			Neighbors: make([][]uint64, level+1),
		}
		// Initialize neighbor lists for each level
		for l := 0; l <= level; l++ {
			node.Neighbors[l] = make([]uint64, 0)
		}
		h.nodes[id] = node
		h.entryPoint = id
		h.maxLevel = level
		h.size++
		return nil
	}

	// Step 4: Search for neighbors at each level from top to bottom
	// Start from entry point at maxLevel
	currentNode := h.entryPoint
	selectedNeighbors := make([][]uint64, level+1) // Neighbors selected at each level

	// Determine the highest level we need to search at (min of maxLevel and level)
	// If new node is at higher level, we only search up to maxLevel (existing graph levels)
	// If new node is at lower level, we search down to its level
	maxSearchLevel := min(h.maxLevel, level)

	// Navigate down from top level to the starting search level
	// Storage cache handles caching efficiently (lookup before lock)
	for searchLevel := h.maxLevel; searchLevel > maxSearchLevel; searchLevel-- {
		// Find nearest neighbor at this level (greedy: ef=1)
		candidates := h.searchLevel(vec, currentNode, searchLevel, 1)
		if len(candidates) > 0 {
			currentNode = candidates[0].id
		}
	}

	// Step 5: For each level from maxSearchLevel down to 0, find neighbors
	// Storage cache handles caching efficiently
	for l := maxSearchLevel; l >= 0; l-- {
		// Search for efConstruction candidates at this level
		candidates := h.searchLevel(vec, currentNode, l, h.efConstruction)
		if len(candidates) == 0 {
			selectedNeighbors[l] = []uint64{}
			continue
		}

		// Select M best neighbors (or all if less than M)
		numNeighbors := h.M
		if len(candidates) < numNeighbors {
			numNeighbors = len(candidates)
		}

		selectedNeighbors[l] = make([]uint64, numNeighbors)
		for i := 0; i < numNeighbors; i++ {
			selectedNeighbors[l][i] = candidates[i].id
		}

		// Update currentNode for next level (use closest candidate)
		if len(candidates) > 0 {
			currentNode = candidates[0].id
		}
	}

	// Step 6: Create new node and connect to selected neighbors
	newNode := &HNSWNode{
		ID:        id,
		Level:     level,
		Neighbors: make([][]uint64, level+1),
	}
	for l := 0; l <= level; l++ {
		if l < len(selectedNeighbors) {
			newNode.Neighbors[l] = make([]uint64, len(selectedNeighbors[l]))
			copy(newNode.Neighbors[l], selectedNeighbors[l])
		} else {
			newNode.Neighbors[l] = make([]uint64, 0)
		}
	}
	h.nodes[id] = newNode

	// Step 7: Update neighbors' connections (bidirectional)
	// For each selected neighbor at each level, add new node as neighbor
	// Then prune neighbors if they exceed M connections
	// Optimization: Cache neighbor vectors to avoid repeated reads during pruning
	for l := 0; l <= level && l < len(selectedNeighbors); l++ {
		for _, neighborID := range selectedNeighbors[l] {
			neighborNode, exists := h.nodes[neighborID]
			if !exists {
				continue
			}

			// Check if neighbor exists at this level
			if neighborNode.Level < l {
				continue
			}

			// Add new node as neighbor (bidirectional connection)
			neighborNode.Neighbors[l] = append(neighborNode.Neighbors[l], id)

			// Prune if neighbor has more than M connections
			if len(neighborNode.Neighbors[l]) > h.M {
				// Get neighbor's vector for distance calculations
				// Storage cache handles caching efficiently (lookup before lock)
				neighborVec, err := h.storage.ReadVector(neighborID)
				if err != nil {
					// If can't read vector, just keep first M
					neighborNode.Neighbors[l] = neighborNode.Neighbors[l][:h.M]
					continue
				}

				// Use candidate heap to find M best neighbors
				candidateHeap := utils.NewCandidateHeap(h.M)
				for _, nID := range neighborNode.Neighbors[l] {
					// Storage cache handles caching efficiently
					nVec, err := h.storage.ReadVector(nID)
					if err != nil {
						continue
					}
					dist := vector.L2Distance(neighborVec, nVec)
					_ = candidateHeap.AddCandidate(utils.Candidate{ID: nID, Distance: dist}, h.M)
				}

				// Extract top M candidates (best first)
				bestCandidates := candidateHeap.ExtractTop(h.M)

				// Update neighbor list with M best neighbors
				neighborNode.Neighbors[l] = make([]uint64, len(bestCandidates))
				for i, cand := range bestCandidates {
					neighborNode.Neighbors[l][i] = cand.ID
				}
			}
		}
	}

	// Step 8: Update entry point if new node is at higher level
	if level > h.maxLevel {
		h.entryPoint = id
		h.maxLevel = level
	}

	h.size++
	return nil
}

// Search finds the k nearest neighbors using HNSW
// Algorithm:
// 1. Start at entryPoint at maxLevel
// 2. Navigate down through levels, finding nearest neighbor at each level
// 3. At level 0, perform thorough search with efSearch candidates
// 4. Return top k results
// Optimized: Pre-allocated slices, early termination, storage-level cache handles vector caching
func (h *HNSWIndex) Search(query []float32, k int) ([]types.SearchResult, error) {
	if len(query) != h.dimension {
		return nil, types.ErrDimensionMismatch
	}

	if k <= 0 {
		return nil, types.ErrInvalidK
	}

	// Empty index
	if h.entryPoint == 0 || len(h.nodes) == 0 {
		return []types.SearchResult{}, nil
	}

	// Step 1: Navigate down from top level to level 1 (greedy search)
	currentNode := h.entryPoint
	for level := h.maxLevel; level > 0; level-- {
		// Find nearest neighbor at this level (greedy: ef=1, just find closest)
		// Storage cache handles caching efficiently (lookup before lock)
		candidates := h.searchLevel(query, currentNode, level, 1)
		if len(candidates) > 0 {
			currentNode = candidates[0].id
		} else {
			// No candidates found, stay at current node
			break
		}
	}

	// Step 2: Search at level 0 with efSearch candidates (thorough search)
	// Storage cache handles caching efficiently
	candidates := h.searchLevel(query, currentNode, 0, h.efSearch)
	if len(candidates) == 0 {
		return []types.SearchResult{}, nil
	}

	// Step 3: Extract top k results
	if k > len(candidates) {
		k = len(candidates)
	}

	// Build results - pre-allocate with exact capacity for better performance
	// Storage cache handles caching efficiently (lookup before lock)
	results := make([]types.SearchResult, 0, k)
	for i := 0; i < len(candidates) && len(results) < k; i++ {
		cand := candidates[i]
		// Storage cache handles caching (lookup before lock, very efficient)
		vec, err := h.storage.ReadVector(cand.id)
		if err != nil {
			// Skip this result if vector can't be read (inconsistent state)
			continue
		}
		// Copy vector to avoid external modifications
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)

		results = append(results, types.SearchResult{
			ID:       cand.id,
			Distance: cand.distance,
			Vector:   vecCopy,
		})
	}

	return results, nil
}

// searchLevel searches for nearest neighbors at a specific level
// Returns candidates sorted by distance (best first)
// Used by Insert to find neighbors at different levels
// Storage handles caching automatically
func (h *HNSWIndex) searchLevel(query []float32, entryNode uint64, level int, ef int) []candidate {
	if ef <= 0 {
		return nil
	}

	// Initialize candidate heap (max-heap to keep worst at top)
	candidateHeap := utils.NewCandidateHeap(ef)
	visited := make(map[uint64]bool, ef*2) // Pre-allocate for better performance
	// Use pre-allocated slice for toVisit to avoid repeated allocations
	toVisit := make([]uint64, 0, ef*2)
	toVisit = append(toVisit, entryNode)

	// Get entry node vector for initial distance
	// Storage handles caching automatically
	entryVector, err := h.storage.ReadVector(entryNode)
	if err != nil {
		return nil // Entry node not found in storage
	}
	entryDist := vector.L2Distance(query, entryVector)
	_ = candidateHeap.AddCandidate(utils.Candidate{ID: entryNode, Distance: entryDist}, ef)
	visited[entryNode] = true

	// Explore graph using greedy search at specified level
	// Reduced max iterations for better performance on large datasets
	maxIterations := ef * 3 // Further reduced for better CPU performance
	iterations := 0
	visitIdx := 0           // Use index instead of slice[1:] to avoid allocations
	noImprovementCount := 0 // Track consecutive iterations with no improvement
	maxNoImprovement := ef  // Early termination if no improvement for this many iterations

	for visitIdx < len(toVisit) && iterations < maxIterations {
		currentID := toVisit[visitIdx]
		visitIdx++
		iterations++

		// Get current node
		currentNode, exists := h.nodes[currentID]
		if !exists {
			continue
		}

		// Check if node exists at this level
		if currentNode.Level < level {
			continue
		}

		// Safety check: ensure Neighbors slice has enough elements
		if level >= len(currentNode.Neighbors) {
			continue
		}

		// Get neighbors at this level
		neighbors := currentNode.Neighbors[level]

		// Track if we found any improvements in this iteration
		improved := false

		// Explore neighbors
		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Get neighbor vector and calculate distance
			// Storage cache handles caching efficiently (lookup before lock)
			neighborVector, err := h.storage.ReadVector(neighborID)
			if err != nil {
				continue // Skip if vector not found
			}
			dist := vector.L2Distance(query, neighborVector)

			// Add to candidate heap
			wasAdded := candidateHeap.AddCandidate(utils.Candidate{ID: neighborID, Distance: dist}, ef)

			// Only add to visit list if candidate was added (it's promising)
			// More selective: only visit if heap not full or if it's significantly better
			if wasAdded {
				improved = true
				heapLen := candidateHeap.Len()
				// Visit if heap not full, or if it's significantly better than worst
				if heapLen < ef {
					toVisit = append(toVisit, neighborID)
				} else if heapLen > 0 {
					// Check if significantly better (within 90% of worst distance)
					worstDist := candidateHeap.Peek().Distance
					if dist < worstDist*0.9 {
						toVisit = append(toVisit, neighborID)
					}
				}
			}
		}

		// Early termination: if no improvement for many iterations, stop exploring
		if improved {
			noImprovementCount = 0
		} else {
			noImprovementCount++
			if noImprovementCount >= maxNoImprovement {
				break // No improvement for too long, stop exploring
			}
		}
	}

	// Extract top candidates (best first)
	topCandidates := candidateHeap.ExtractTop(ef)
	// Convert utils.Candidate to local candidate type for return
	candidates := make([]candidate, len(topCandidates))
	for i, c := range topCandidates {
		candidates[i] = candidate{id: c.ID, distance: c.Distance}
	}
	return candidates
}

// ReadVector retrieves a vector by ID from storage
// Storage handles caching automatically
func (h *HNSWIndex) ReadVector(id uint64) ([]float32, error) {
	if h.storage == nil {
		return nil, errors.New("storage not available")
	}
	// Optional: Check if node exists in graph (fast map lookup, similar to Flat)
	// This provides consistency but doesn't affect performance significantly
	if _, exists := h.nodes[id]; !exists {
		return nil, fmt.Errorf("vector with ID %d not found in index", id)
	}
	// Storage handles caching automatically (same as Flat)
	return h.storage.ReadVector(id)
}

// Delete removes a vector from the HNSW index
// 1. Deletes the vector from storage (marks as tombstone in db file)
// 2. Removes the node from the graph structure
// 3. Removes all references to this node from other nodes' neighbor lists
// 4. Updates entry point if it was the deleted node
func (h *HNSWIndex) Delete(id uint64) error {
	// Check if node exists in graph
	_, exists := h.nodes[id]
	if !exists {
		// Node doesn't exist in graph, but try to delete from storage anyway
		// (in case storage has it but graph doesn't)
		if h.storage != nil {
			_ = h.storage.DeleteVector(id)
		}
		return nil
	}

	// Step 1: Delete vector from storage (marks as tombstone)
	if h.storage != nil {
		if err := h.storage.DeleteVector(id); err != nil {
			return fmt.Errorf("failed to delete vector from storage: %w", err)
		}
	}

	// Step 2: Remove this node from all other nodes' neighbor lists
	// Iterate through all nodes and remove references to the deleted node
	for otherID, otherNode := range h.nodes {
		if otherID == id {
			continue // Skip the node being deleted
		}

		// Remove from all levels where this node appears
		for level := 0; level <= otherNode.Level; level++ {
			neighbors := otherNode.Neighbors[level]
			// Find and remove the deleted node ID (order doesn't matter in HNSW)
			for i, neighborID := range neighbors {
				if neighborID == id {
					// Swap with last element and truncate (O(1) instead of O(n))
					lastIdx := len(neighbors) - 1
					neighbors[i] = neighbors[lastIdx]
					otherNode.Neighbors[level] = neighbors[:lastIdx]
					break // Found and removed, no need to continue
				}
			}
		}
	}

	// Step 3: Update entry point if it was the deleted node
	if h.entryPoint == id {
		// Find a new entry point from remaining nodes
		// Prefer a node at the highest level
		h.entryPoint = 0
		h.maxLevel = -1
		for otherID, otherNode := range h.nodes {
			if otherID != id && otherNode.Level > h.maxLevel {
				h.maxLevel = otherNode.Level
				h.entryPoint = otherID
			}
		}
		// If no nodes left, reset entry point
		if len(h.nodes) == 1 { // Only the deleted node remains
			h.entryPoint = 0
			h.maxLevel = -1
		}
	}

	// Step 4: Remove node from graph
	delete(h.nodes, id)
	h.size = len(h.nodes)

	return nil
}

// Size returns the number of vectors in the index
func (h *HNSWIndex) Size() int {
	return len(h.nodes) // Use map length instead of maintaining separate counter
}

// Clear removes all vectors from the index
// 1. Empties the graph (removes all nodes)
// 2. Removes all vectors from storage (clears db file)
// 3. Resets entryPoint to 0 and maxLevel to -1
func (h *HNSWIndex) Clear() error {
	// Step 1: Clear all nodes from graph
	h.nodes = make(map[uint64]*HNSWNode)
	h.size = 0

	// Step 2: Clear all vectors from storage
	if h.storage != nil {
		if err := h.storage.Clear(); err != nil {
			return fmt.Errorf("failed to clear storage: %w", err)
		}
	}

	// Step 3: Reset entry point and max level
	h.entryPoint = 0
	h.maxLevel = -1

	// Note: Cache clearing is handled by storage.Clear()

	return nil
}
