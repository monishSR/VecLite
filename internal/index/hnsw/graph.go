package hnsw

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/monishSR/veclite/internal/storage"
)

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
