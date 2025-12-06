package hnsw

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/msr23/veclite/internal/index/types"
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

// SaveGraph saves the HNSW graph structure to disk
// Graph file path is automatically derived from storage file path by appending ".graph"
func (h *HNSWIndex) SaveGraph() error {
	if h.storage == nil {
		return errors.New("storage is required to save graph")
	}

	// Derive graph path from storage file path
	storagePath := h.storage.GetFilePath()
	graphPath := storagePath + ".graph"

	file, err := os.Create(graphPath)
	if err != nil {
		return fmt.Errorf("failed to create graph file: %w", err)
	}
	defer file.Close()

	// Write magic number for validation
	magic := uint32(0x48534E57) // "HNSW" in ASCII
	if err := binary.Write(file, binary.LittleEndian, magic); err != nil {
		return err
	}

	// Write version (for future compatibility)
	version := uint32(1)
	if err := binary.Write(file, binary.LittleEndian, version); err != nil {
		return err
	}

	// Write parameters
	if err := binary.Write(file, binary.LittleEndian, uint32(h.dimension)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(h.M)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(h.efConstruction)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(h.efSearch)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.mL); err != nil {
		return err
	}

	// Write graph metadata
	if err := binary.Write(file, binary.LittleEndian, h.entryPoint); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, int32(h.maxLevel)); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(h.nodes))); err != nil {
		return err
	}

	// Write each node
	for id, node := range h.nodes {
		// Write node ID
		if err := binary.Write(file, binary.LittleEndian, id); err != nil {
			return err
		}

		// Write node level
		if err := binary.Write(file, binary.LittleEndian, int32(node.Level)); err != nil {
			return err
		}

		// Write neighbors for each level (0 to node.Level)
		for level := 0; level <= node.Level; level++ {
			neighbors := node.Neighbors[level]
			if err := binary.Write(file, binary.LittleEndian, int32(level)); err != nil {
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, uint32(len(neighbors))); err != nil {
				return err
			}
			for _, neighborID := range neighbors {
				if err := binary.Write(file, binary.LittleEndian, neighborID); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// LoadGraph loads the HNSW graph structure from disk
// Graph file path is automatically derived from storage file path by appending ".graph"
func (h *HNSWIndex) LoadGraph() error {
	if h.storage == nil {
		return errors.New("storage is required to load graph")
	}

	// Derive graph path from storage file path
	storagePath := h.storage.GetFilePath()
	graphPath := storagePath + ".graph"

	file, err := os.Open(graphPath)
	if err != nil {
		return fmt.Errorf("failed to open graph file: %w", err)
	}
	defer file.Close()

	// Read and validate magic number
	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != 0x48534E57 { // "HNSW"
		return fmt.Errorf("invalid graph file: magic number mismatch")
	}

	// Read version
	var version uint32
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != 1 {
		return fmt.Errorf("unsupported graph file version: %d", version)
	}

	// Read parameters
	var dim, M, efConstruction, efSearch uint32
	var mL float64
	if err := binary.Read(file, binary.LittleEndian, &dim); err != nil {
		return fmt.Errorf("failed to read dimension: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &M); err != nil {
		return fmt.Errorf("failed to read M: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &efConstruction); err != nil {
		return fmt.Errorf("failed to read efConstruction: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &efSearch); err != nil {
		return fmt.Errorf("failed to read efSearch: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &mL); err != nil {
		return fmt.Errorf("failed to read mL: %w", err)
	}

	// Set all parameters from graph file (source of truth)
	h.dimension = int(dim)
	h.M = int(M)
	h.efConstruction = int(efConstruction)
	h.efSearch = int(efSearch)
	h.mL = mL

	// Update config map for consistency
	if h.config == nil {
		h.config = make(map[string]any)
	}
	h.config["M"] = int(M)
	h.config["EfConstruction"] = int(efConstruction)
	h.config["EfSearch"] = int(efSearch)

	// Read graph metadata
	var entryPoint uint64
	var maxLevel int32
	var nodeCount uint32
	if err := binary.Read(file, binary.LittleEndian, &entryPoint); err != nil {
		return fmt.Errorf("failed to read entry point: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &maxLevel); err != nil {
		return fmt.Errorf("failed to read max level: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &nodeCount); err != nil {
		return fmt.Errorf("failed to read node count: %w", err)
	}

	h.entryPoint = entryPoint
	h.maxLevel = int(maxLevel)
	h.nodes = make(map[uint64]*HNSWNode, nodeCount)

	// Read each node
	for i := uint32(0); i < nodeCount; i++ {
		var id uint64
		var level int32
		if err := binary.Read(file, binary.LittleEndian, &id); err != nil {
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF while reading node %d", i)
			}
			return fmt.Errorf("failed to read node ID: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &level); err != nil {
			return fmt.Errorf("failed to read node level: %w", err)
		}

		node := &HNSWNode{
			ID:        id,
			Level:     int(level),
			Neighbors: make([][]uint64, level+1),
		}

		// Read neighbors for each level
		for l := int32(0); l <= level; l++ {
			var actualLevel int32
			var neighborCount uint32
			if err := binary.Read(file, binary.LittleEndian, &actualLevel); err != nil {
				return fmt.Errorf("failed to read level for node %d: %w", id, err)
			}
			if actualLevel != l {
				return fmt.Errorf("level mismatch for node %d: expected %d, got %d", id, l, actualLevel)
			}
			if err := binary.Read(file, binary.LittleEndian, &neighborCount); err != nil {
				return fmt.Errorf("failed to read neighbor count: %w", err)
			}

			neighbors := make([]uint64, neighborCount)
			for j := uint32(0); j < neighborCount; j++ {
				if err := binary.Read(file, binary.LittleEndian, &neighbors[j]); err != nil {
					return fmt.Errorf("failed to read neighbor %d for node %d: %w", j, id, err)
				}
			}
			node.Neighbors[int(l)] = neighbors
		}

		h.nodes[id] = node
	}

	h.size = len(h.nodes)
	return nil
}

// OpenHNSWIndex opens an existing HNSW index and loads the graph structure from disk
// All parameters (dimension, M, efConstruction, efSearch, mL) are loaded from the graph file
// Graph file path is automatically derived from storage file path by appending ".graph"
// If graph file doesn't exist, returns an error (use NewHNSWIndex for new indexes)
func OpenHNSWIndex(storage *storage.Storage) (*HNSWIndex, error) {
	if storage == nil {
		return nil, errors.New("storage is required for OpenHNSWIndex")
	}

	// Create a minimal index structure - parameters will be loaded from graph file
	h := &HNSWIndex{
		storage: storage,
		nodes:   make(map[uint64]*HNSWNode),
		config:  make(map[string]any),
	}

	// Load graph from disk (this will populate all parameters)
	if err := h.LoadGraph(); err != nil {
		return nil, fmt.Errorf("failed to load graph: %w", err)
	}

	return h, nil
}

// Insert adds a vector to the HNSW index
func (h *HNSWIndex) Insert(id uint64, vector []float32) error {
	if len(vector) != h.dimension {
		return types.ErrDimensionMismatch
	}

	// TODO: Implement HNSW insert
	return errors.New("HNSW index not yet implemented")
}

// Search finds the k nearest neighbors using HNSW
// Algorithm:
// 1. Start at entryPoint at maxLevel
// 2. Navigate down through levels, finding nearest neighbor at each level
// 3. At level 0, perform thorough search with efSearch candidates
// 4. Return top k results
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
		candidates := h.searchLevel(query, currentNode, level, 1)
		if len(candidates) > 0 {
			currentNode = candidates[0].id
		} else {
			// No candidates found, stay at current node
			break
		}
	}

	// Step 2: Search at level 0 with efSearch candidates (thorough search)
	candidates := h.searchLevel(query, currentNode, 0, h.efSearch)
	if len(candidates) == 0 {
		return []types.SearchResult{}, nil
	}

	// Step 3: Extract top k results
	if k > len(candidates) {
		k = len(candidates)
	}

	results := make([]types.SearchResult, k)
	for i := 0; i < k; i++ {
		cand := candidates[i]
		// Storage handles caching automatically
		vec, err := h.storage.ReadVector(cand.id)
		if err != nil {
			return nil, fmt.Errorf("failed to read vector %d: %w", cand.id, err)
		}
		// Copy vector to avoid external modifications
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)

		results[i] = types.SearchResult{
			ID:       cand.id,
			Distance: cand.distance,
			Vector:   vecCopy,
		}
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
	candidateHeap := newCandidateHeap(ef)
	visited := make(map[uint64]bool)
	toVisit := []uint64{entryNode}

	// Get entry node vector for initial distance
	// Storage handles caching automatically
	entryVector, err := h.storage.ReadVector(entryNode)
	if err != nil {
		return nil // Entry node not found in storage
	}
	entryDist := vector.L2Distance(query, entryVector)
	candidateHeap.AddCandidate(candidate{id: entryNode, distance: entryDist}, ef)
	visited[entryNode] = true

	// Explore graph using greedy search at specified level
	for len(toVisit) > 0 {
		currentID := toVisit[0]
		toVisit = toVisit[1:]

		// Get current node
		currentNode, exists := h.nodes[currentID]
		if !exists {
			continue
		}

		// Check if node exists at this level
		if currentNode.Level < level {
			continue
		}

		// Get neighbors at this level
		neighbors := currentNode.Neighbors[level]

		// Explore neighbors
		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Get neighbor vector and calculate distance
			// Storage handles caching automatically
			neighborVector, err := h.storage.ReadVector(neighborID)
			if err != nil {
				continue // Skip if vector not found
			}
			dist := vector.L2Distance(query, neighborVector)

			// Add to candidate heap
			candidateHeap.AddCandidate(candidate{id: neighborID, distance: dist}, ef)

			// Add to visit list if it's better than worst in heap
			if candidateHeap.Len() < ef || dist < candidateHeap.Peek().distance {
				toVisit = append(toVisit, neighborID)
			}
		}
	}

	// Extract top candidates (best first)
	return candidateHeap.ExtractTop(ef)
}

// ReadVector retrieves a vector by ID from storage
// Storage handles caching automatically
func (h *HNSWIndex) ReadVector(id uint64) ([]float32, error) {
	if h.storage == nil {
		return nil, errors.New("storage not available")
	}
	// Storage handles caching automatically
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

