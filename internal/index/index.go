package index

import (
	"errors"

	"github.com/msr23/veclite/internal/index/flat"
	"github.com/msr23/veclite/internal/index/hnsw"
	"github.com/msr23/veclite/internal/index/ivf"
	"github.com/msr23/veclite/internal/index/types"
	"github.com/msr23/veclite/internal/storage"
)

// Index is the interface for vector indexing structures
type Index interface {
	Insert(id uint64, vector []float32) error
	Search(query []float32, k int) ([]types.SearchResult, error)
	ReadVector(id uint64) ([]float32, error) // Read vector by ID
	Delete(id uint64) error                  // Delete vector by ID
	Size() int                               // Get number of vectors
	Clear() error                            // Clear all vectors
}

// SearchResult is an alias to types.SearchResult for convenience
type SearchResult = types.SearchResult

// Re-export errors for convenience
var (
	ErrDimensionMismatch = types.ErrDimensionMismatch
	ErrInvalidK          = types.ErrInvalidK
)

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
		return hnsw.NewHNSWIndex(dimension, config, storage)
	case IndexTypeFlat:
		return flat.NewFlatIndex(dimension, storage), nil
	case IndexTypeIVF:
		return ivf.NewIVFIndex(dimension, config, storage)
	default:
		return nil, errors.New("unknown index type")
	}
}
