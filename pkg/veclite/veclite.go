package veclite

import (
	"errors"
	"fmt"
	"sync"

	"github.com/msr23/veclite/internal/index"
	"github.com/msr23/veclite/internal/storage"
)

// VecLite represents the main embedded vector database instance
type VecLite struct {
	mu      sync.RWMutex // Read-write lock for thread safety
	config  *Config
	storage *storage.Storage
	index   index.Index // Abstract index interface
}

// Config holds configuration for VecLite
type Config struct {
	DataPath       string
	Dimension      int
	IndexType      string
	MaxElements    int
	M              int // HNSW parameter
	EfConstruction int // HNSW parameter
	EfSearch       int // HNSW parameter
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		DataPath:    "./veclite.db",
		Dimension:   128,
		IndexType:   "flat",
		MaxElements: 10000,
	}
}

// New creates a new VecLite instance
func New(config *Config) (*VecLite, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if config.Dimension <= 0 {
		return nil, errors.New("dimension must be greater than 0")
	}

	// Initialize storage
	store, err := storage.NewStorage(config.DataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}
	if err := store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	// Initialize index based on config
	indexConfig := make(map[string]any)
	indexConfig["M"] = config.M
	indexConfig["MaxElements"] = config.MaxElements

	// Pass storage to index (indexes can use it or ignore it)
	idx, err := index.NewIndex(index.IndexType(config.IndexType), config.Dimension, indexConfig, store)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	return &VecLite{
		config:  config,
		storage: store,
		index:   idx,
	}, nil
}

// Open opens an existing VecLite database
func Open(dataPath string) (*VecLite, error) {
	config := DefaultConfig()
	config.DataPath = dataPath

	// TODO: Load existing database
	return New(config)
}

// Close closes the database and flushes all data to disk
// Requires exclusive lock to ensure no operations are in progress
func (v *VecLite) Close() error {
	v.mu.Lock() // Exclusive lock - wait for all operations to complete
	defer v.mu.Unlock()

	if v.storage != nil {
		if err := v.storage.Sync(); err != nil {
			return err
		}
		return v.storage.Close()
	}
	return nil
}

// Insert adds a vector with an ID to the database
// Requires exclusive write lock - blocks all reads and other writes
func (v *VecLite) Insert(id uint64, vector []float32) error {
	if len(vector) != v.config.Dimension {
		return fmt.Errorf("vector dimension %d does not match configured dimension %d", len(vector), v.config.Dimension)
	}

	v.mu.Lock() // Exclusive write lock
	defer v.mu.Unlock()

	if err := v.index.Insert(id, vector); err != nil {
		return err
	}
	return nil
}

// Search finds the k nearest neighbors to a query vector
// Uses read lock - allows multiple concurrent searches
func (v *VecLite) Search(query []float32, k int) ([]index.SearchResult, error) {
	if len(query) != v.config.Dimension {
		return nil, fmt.Errorf("query dimension %d does not match configured dimension %d", len(query), v.config.Dimension)
	}

	if k <= 0 {
		return nil, errors.New("k must be greater than 0")
	}

	v.mu.RLock() // Shared read lock - multiple readers allowed
	defer v.mu.RUnlock()

	return v.index.Search(query, k)
}

// Delete removes a vector by ID
// Requires exclusive write lock - blocks all reads and other writes
func (v *VecLite) Delete(id uint64) error {
	v.mu.Lock() // Exclusive write lock
	defer v.mu.Unlock()

	return v.index.Delete(id)
}

// Get retrieves a vector by ID
// Uses read lock - allows multiple concurrent reads
func (v *VecLite) Get(id uint64) ([]float32, error) {
	v.mu.RLock() // Shared read lock
	defer v.mu.RUnlock()

	return v.index.ReadVector(id)
}

// Size returns the number of vectors in the database
// Uses read lock - allows concurrent reads
func (v *VecLite) Size() int {
	v.mu.RLock() // Shared read lock
	defer v.mu.RUnlock()

	return v.index.Size()
}

// SearchResult is an alias to index.SearchResult for convenience
type SearchResult = index.SearchResult
