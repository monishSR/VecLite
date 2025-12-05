package index

import (
	"errors"

	"github.com/msr23/veclite/internal/storage"
)

// Index is the interface for vector indexing structures
type Index interface {
	Insert(id uint64, vector []float32) error
	Search(query []float32, k int) ([]SearchResult, error)
	ReadVector(id uint64) ([]float32, error) // Read vector by ID
	Delete(id uint64) error                  // Delete vector by ID
	Size() int                               // Get number of vectors
	Clear() error                            // Clear all vectors
}

// SearchResult represents a search result with ID, distance, and vector
type SearchResult struct {
	ID       uint64
	Distance float32
	Vector   []float32
}

// IndexType represents the type of index
type IndexType string

const (
	IndexTypeHNSW IndexType = "hnsw"
	IndexTypeIVF  IndexType = "ivf"
	IndexTypeFlat IndexType = "flat"
)

// NewIndex creates a new index based on the index type
// storage can be nil for indexes that don't need it (e.g., memory-only FlatIndex)
func NewIndex(indexType IndexType, dimension int, config map[string]any, storage *storage.Storage) (Index, error) {
	switch indexType {
	case IndexTypeHNSW:
		return NewHNSWIndex(dimension, config, storage)
	case IndexTypeFlat:
		return NewFlatIndex(dimension, storage), nil
	case IndexTypeIVF:
		return NewIVFIndex(dimension, config, storage)
	default:
		return nil, errors.New("unknown index type")
	}
}
