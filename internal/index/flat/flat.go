package flat

import (
	"errors"
	"fmt"
	"sort"

	"github.com/monishSR/veclite/internal/index/types"
	"github.com/monishSR/veclite/internal/storage"
	"github.com/monishSR/veclite/internal/vector"
)

// FlatIndex is a simple brute-force index
// Uses storage for persistence and relies on storage cache for performance
// storage is required - vectors are stored on disk and accessed via cache
type FlatIndex struct {
	dimension int
	ids       map[uint64]bool  // Track which IDs exist (for Size and iteration)
	storage   *storage.Storage // Required storage
}

// NewFlatIndex creates a new flat index
// storage is required - vectors are stored on disk and accessed via cache
func NewFlatIndex(dimension int, storage *storage.Storage) *FlatIndex {
	return &FlatIndex{
		dimension: dimension,
		ids:       make(map[uint64]bool),
		storage:   storage,
	}
}

// OpenFlatIndex opens an existing flat index and loads all vector IDs from storage.
func OpenFlatIndex(dimension int, storage *storage.Storage) (*FlatIndex, error) {
	if storage == nil {
		return nil, errors.New("storage is required for OpenFlatIndex")
	}

	f := &FlatIndex{
		dimension: dimension,
		ids:       make(map[uint64]bool),
		storage:   storage,
	}

	// Load all vectors from storage to populate IDs (vectors are read via storage.ReadVector later)
	vectors, err := storage.ReadAllVectors()
	if err != nil {
		return nil, fmt.Errorf("failed to load vector IDs from storage: %w", err)
	}

	// Validate dimension and populate IDs
	for id, vec := range vectors {
		if len(vec) != dimension {
			return nil, fmt.Errorf("vector dimension mismatch: expected %d, got %d for ID %d", dimension, len(vec), id)
		}
		f.ids[id] = true
	}

	return f, nil
}

// Insert adds a vector to the index.
// It writes the vector to storage and records its ID.
func (f *FlatIndex) Insert(id uint64, vec []float32) error {
	if len(vec) != f.dimension {
		return types.ErrDimensionMismatch
	}
	if f.storage == nil {
		return errors.New("storage not available for FlatIndex")
	}

	if err := f.storage.WriteVector(id, vec); err != nil {
		return err
	}
	f.ids[id] = true // Record the ID
	return nil
}

// Search finds the k nearest neighbors using brute force.
// It reads vectors from storage (which uses the cache).
func (f *FlatIndex) Search(query []float32, k int) ([]types.SearchResult, error) {
	if len(query) != f.dimension {
		return nil, types.ErrDimensionMismatch
	}
	if k <= 0 {
		return nil, types.ErrInvalidK
	}
	if f.storage == nil {
		return nil, errors.New("storage not available for FlatIndex")
	}

	type result struct {
		id       uint64
		distance float32
		vec      []float32
	}

	results := make([]result, 0, len(f.ids))
	for id := range f.ids {
		vec, err := f.storage.ReadVector(id)
		if err != nil {
			// Log error but continue if a single vector read fails
			fmt.Printf("Warning: Failed to read vector %d from storage during search: %v\n", id, err)
			continue
		}
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

	searchResults := make([]types.SearchResult, k)
	for i := 0; i < k; i++ {
		searchResults[i] = types.SearchResult{
			ID:       results[i].id,
			Distance: results[i].distance,
			Vector:   results[i].vec,
		}
	}

	return searchResults, nil
}

// ReadVector retrieves a vector by ID from storage.
func (f *FlatIndex) ReadVector(id uint64) ([]float32, error) {
	if f.storage == nil {
		return nil, errors.New("storage not available for FlatIndex")
	}
	if _, exists := f.ids[id]; !exists {
		return nil, fmt.Errorf("vector with ID %d not found in index", id)
	}
	return f.storage.ReadVector(id)
}

// Delete removes a vector from the index and storage.
func (f *FlatIndex) Delete(id uint64) error {
	if f.storage == nil {
		return errors.New("storage not available for FlatIndex")
	}
	delete(f.ids, id) // Remove from in-memory ID set
	return f.storage.DeleteVector(id)
}

// Size returns the number of vectors in the index.
func (f *FlatIndex) Size() int {
	return len(f.ids)
}

// Clear removes all vectors from the index and storage.
func (f *FlatIndex) Clear() error {
	if f.storage == nil {
		return errors.New("storage not available for FlatIndex")
	}
	// Clear storage (cache clearing handled by storage)
	if err := f.storage.Clear(); err != nil {
		return err
	}

	// Clear ID tracking
	f.ids = make(map[uint64]bool)

	return nil
}
