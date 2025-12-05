package index

import (
	"errors"
	"fmt"
	"sort"

	"github.com/msr23/veclite/internal/storage"
	"github.com/msr23/veclite/internal/vector"
)

// FlatIndex is a simple brute-force index that stores all vectors
// storage is optional - if nil, all vectors are kept in memory
type FlatIndex struct {
	dimension int
	vectors   map[uint64][]float32
	storage   *storage.Storage // Optional storage for hybrid mode
}

// NewFlatIndex creates a new flat index
// storage can be nil for memory-only mode
func NewFlatIndex(dimension int, storage *storage.Storage) *FlatIndex {
	return &FlatIndex{
		dimension: dimension,
		vectors:   make(map[uint64][]float32),
		storage:   storage,
	}
}

// OpenFlatIndex opens an existing flat index and loads all vectors from storage into memory
func OpenFlatIndex(dimension int, storage *storage.Storage) (*FlatIndex, error) {
	if storage == nil {
		return nil, errors.New("storage is required for OpenFlatIndex")
	}

	f := &FlatIndex{
		dimension: dimension,
		vectors:   make(map[uint64][]float32),
		storage:   storage,
	}

	// Load all vectors from storage into memory
	vectors, err := storage.ReadAllVectors()
	if err != nil {
		return nil, fmt.Errorf("failed to load vectors from storage: %w", err)
	}

	// Validate dimension and copy vectors
	for id, vec := range vectors {
		if len(vec) != dimension {
			return nil, fmt.Errorf("vector dimension mismatch: expected %d, got %d for ID %d", dimension, len(vec), id)
		}
		// Copy to avoid external modifications
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)
		f.vectors[id] = vecCopy
	}

	return f, nil
}

// Insert adds a vector to the index
func (f *FlatIndex) Insert(id uint64, vec []float32) error {
	if len(vec) != f.dimension {
		return ErrDimensionMismatch
	}

	// Copy the vector to avoid external modifications
	vecCopy := make([]float32, len(vec))
	copy(vecCopy, vec)
	f.vectors[id] = vecCopy

	// Optionally write to storage
	if f.storage != nil {
		return f.storage.WriteVector(id, vec)
	}
	return nil
}

// Search finds the k nearest neighbors using brute force
func (f *FlatIndex) Search(query []float32, k int) ([]SearchResult, error) {
	if len(query) != f.dimension {
		return nil, ErrDimensionMismatch
	}

	if k <= 0 {
		return nil, ErrInvalidK
	}

	// Calculate distances to all vectors
	type result struct {
		id       uint64
		distance float32
		vec      []float32
	}

	results := make([]result, 0, len(f.vectors))
	for id, vec := range f.vectors {
		dist := vector.L2Distance(query, vec)
		// Copy vector to avoid external modifications
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)
		results = append(results, result{id: id, distance: dist, vec: vecCopy})
	}

	// Sort by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	// Return top k
	if k > len(results) {
		k = len(results)
	}

	searchResults := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		searchResults[i] = SearchResult{
			ID:       results[i].id,
			Distance: results[i].distance,
			Vector:   results[i].vec,
		}
	}

	return searchResults, nil
}

// ReadVector retrieves a vector by ID
func (f *FlatIndex) ReadVector(id uint64) ([]float32, error) {
	// Try memory first
	if vec, exists := f.vectors[id]; exists {
		// Return a copy to avoid external modifications
		vecCopy := make([]float32, len(vec))
		copy(vecCopy, vec)
		return vecCopy, nil
	}

	// If not in memory and storage exists, try storage
	if f.storage != nil {
		return f.storage.ReadVector(id)
	}

	return nil, fmt.Errorf("vector with ID %d not found", id)
}

// Delete removes a vector from the index
func (f *FlatIndex) Delete(id uint64) error {
	delete(f.vectors, id)
	if f.storage != nil {
		return f.storage.DeleteVector(id)
	}
	return nil
}

// Size returns the number of vectors in the index
func (f *FlatIndex) Size() int {
	return len(f.vectors)
}

// Clear removes all vectors from the index
func (f *FlatIndex) Clear() error {
	f.vectors = make(map[uint64][]float32)
	if f.storage != nil {
		return f.storage.Clear()
	}
	return nil
}

var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
	ErrInvalidK          = errors.New("k must be greater than 0")
)
