package ivf

import (
	"errors"
	"fmt"
	"sort"

	"github.com/monishSR/veclite/internal/index/types"
	"github.com/monishSR/veclite/internal/storage"
	"github.com/monishSR/veclite/internal/vector"
)

// IVFIndex implements Inverted File index
// Memory-efficient: only stores cluster structure, vectors in storage
type IVFIndex struct {
	dimension int
	config    map[string]any
	storage   *storage.Storage // Storage for all vectors (including centroids)

	// IVF-specific structures (memory-efficient: only IDs)
	centroids       []Centroid       // Cluster centroids (only IDs, vectors in storage)
	clusters        map[int][]uint64 // clusterID -> vector IDs in this cluster
	vectorToCluster map[uint64]int   // vectorID -> clusterID (for fast lookup)
	size            int              // Total number of vectors

	// IVF parameters
	nClusters int // Number of clusters (typically √N to N/10)
	nProbe    int // Number of clusters to search during query (default: 1)
}

// NewIVFIndex creates a new IVF index
// storage is required for IVF to store vectors on disk
func NewIVFIndex(dimension int, config map[string]any, storage *storage.Storage) (*IVFIndex, error) {
	if dimension <= 0 {
		return nil, errors.New("dimension must be greater than 0")
	}

	// Extract IVF parameters from config
	nClusters := 100 // Default: will be adjusted based on dataset size
	if nc, ok := config["NClusters"].(int); ok && nc > 0 {
		nClusters = nc
	}

	nProbe := 1 // Default: search 1 cluster
	if np, ok := config["NProbe"].(int); ok && np > 0 {
		nProbe = np
	}

	return &IVFIndex{
		dimension:       dimension,
		config:          config,
		storage:         storage,
		centroids:       make([]Centroid, 0),
		clusters:        make(map[int][]uint64),
		vectorToCluster: make(map[uint64]int),
		size:            0,
		nClusters:       nClusters,
		nProbe:          nProbe,
	}, nil
}

// OpenIVFIndex opens an existing IVF index and loads the structure from disk
// All parameters (dimension, nClusters, nProbe) are loaded from the IVF file
// IVF file path is automatically derived from storage file path by appending ".ivf"
// If IVF file doesn't exist, returns an error (use NewIVFIndex for new indexes)
func OpenIVFIndex(storage *storage.Storage) (*IVFIndex, error) {
	if storage == nil {
		return nil, errors.New("storage is required for OpenIVFIndex")
	}

	// Create a minimal index structure - parameters will be loaded from IVF file
	i := &IVFIndex{
		storage: storage,
		config:  make(map[string]any),
	}

	// Load IVF from disk (this will populate all parameters)
	if err := i.LoadIVF(); err != nil {
		return nil, fmt.Errorf("failed to load IVF: %w", err)
	}

	return i, nil
}

// Insert adds a vector to the IVF index
func (i *IVFIndex) Insert(id uint64, vector []float32) error {
	if len(vector) != i.dimension {
		return types.ErrDimensionMismatch
	}

	if i.storage == nil {
		return errors.New("storage not available")
	}

	// Write vector to storage
	if err := i.storage.WriteVector(id, vector); err != nil {
		return fmt.Errorf("failed to write vector to storage: %w", err)
	}

	// Handle initialization phase: no centroids exist yet
	if len(i.centroids) == 0 {
		return i.initializeFirstCentroid(id, vector)
	}

	// Handle initialization phase: still building centroids
	if len(i.centroids) < i.nClusters {
		return i.addCentroidFromVector(id, vector)
	}

	// Normal insertion: centroids exist, find nearest and assign
	clusterID := i.findNearestCentroid(vector)
	i.clusters[clusterID] = append(i.clusters[clusterID], id)
	i.vectorToCluster[id] = clusterID
	i.updateCentroid(clusterID, vector)
	i.size++
	return nil
}

// Search finds the k nearest neighbors using IVF
// Algorithm:
// 1. Find nProbe nearest centroids to the query
// 2. Search vectors in those selected clusters
// 3. Compute distances to all vectors in those clusters
// 4. Sort and return top k results
func (i *IVFIndex) Search(query []float32, k int) ([]types.SearchResult, error) {
	if len(query) != i.dimension {
		return nil, types.ErrDimensionMismatch
	}

	if k <= 0 {
		return nil, types.ErrInvalidK
	}

	if i.storage == nil {
		return nil, errors.New("storage not available")
	}

	// Empty index
	if i.size == 0 || len(i.centroids) == 0 {
		return []types.SearchResult{}, nil
	}

	// Find nProbe nearest clusters
	nearestClusters := i.findNearestClusters(query, i.nProbe)
	if len(nearestClusters) == 0 {
		return []types.SearchResult{}, nil
	}

	// Search vectors in selected clusters
	candidates := make([]types.SearchResult, 0)

	for _, clusterID := range nearestClusters {
		// Get all vector IDs in this cluster
		clusterVectors := i.clusters[clusterID]
		for _, vecID := range clusterVectors {
			// Skip centroid IDs (they're in high ID range)
			// Centroids are stored with IDs from allocateCentroidID
			const centroidIDBase = ^uint64(0)
			if vecID >= centroidIDBase-uint64(len(i.centroids)) {
				continue // Skip centroid vectors
			}

			// Load vector from storage (cache handles caching automatically)
			vec, err := i.storage.ReadVector(vecID)
			if err != nil {
				// Log error but continue if a single vector read fails
				continue
			}

			dist := vector.L2Distance(query, vec)
			// Copy vector to avoid external modifications
			vecCopy := make([]float32, len(vec))
			copy(vecCopy, vec)
			candidates = append(candidates, types.SearchResult{
				ID:       vecID,
				Distance: dist,
				Vector:   vecCopy,
			})
		}
	}

	// Sort by distance (best first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Distance < candidates[j].Distance
	})

	// Return top k
	if k > len(candidates) {
		k = len(candidates)
	}

	return candidates[:k], nil
}

// ReadVector retrieves a vector by ID from storage
func (i *IVFIndex) ReadVector(id uint64) ([]float32, error) {
	if i.storage == nil {
		return nil, errors.New("storage not available")
	}
	// Check if vector exists in index (fast map lookup)
	if _, exists := i.vectorToCluster[id]; !exists {
		return nil, fmt.Errorf("vector with ID %d not found in index", id)
	}
	// Storage handles caching automatically
	return i.storage.ReadVector(id)
}

// Delete removes a vector from the IVF index
// 1. Removes vector from cluster assignment
// 2. Updates centroid (recomputes without deleted vector)
// 3. Deletes vector from storage
// 4. Removes from vectorToCluster map
func (i *IVFIndex) Delete(id uint64) error {
	if i.storage == nil {
		return errors.New("storage not available")
	}

	// Check if vector exists in index
	clusterID, exists := i.vectorToCluster[id]
	if !exists {
		// Vector doesn't exist in index, but try to delete from storage anyway
		// (in case storage has it but index doesn't)
		_ = i.storage.DeleteVector(id)
		return nil
	}

	// Step 1: Remove vector from cluster
	cluster := i.clusters[clusterID]
	for j, vecID := range cluster {
		if vecID == id {
			// Remove from cluster (swap with last element and truncate)
			lastIdx := len(cluster) - 1
			cluster[j] = cluster[lastIdx]
			i.clusters[clusterID] = cluster[:lastIdx]
			break
		}
	}

	// Step 2: Update centroid (recompute without deleted vector)
	// Load all remaining vectors in cluster and recompute centroid
	if len(i.clusters[clusterID]) > 0 {
		i.recomputeCentroid(clusterID)
	}

	// Step 3: Delete vector from storage
	if err := i.storage.DeleteVector(id); err != nil {
		return fmt.Errorf("failed to delete vector from storage: %w", err)
	}

	// Step 4: Remove from vectorToCluster map
	delete(i.vectorToCluster, id)
	i.size--

	return nil
}

// Size returns the number of vectors in the index
func (i *IVFIndex) Size() int {
	return i.size
}

// Clear removes all vectors from the index
// Clears all cluster structures and storage
func (i *IVFIndex) Clear() error {
	if i.storage == nil {
		return errors.New("storage not available")
	}

	// Clear storage (cache clearing handled by storage)
	if err := i.storage.Clear(); err != nil {
		return fmt.Errorf("failed to clear storage: %w", err)
	}

	// Clear all IVF structures
	i.centroids = make([]Centroid, 0)
	i.clusters = make(map[int][]uint64)
	i.vectorToCluster = make(map[uint64]int)
	i.size = 0

	return nil
}
