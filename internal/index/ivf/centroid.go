package ivf

import (
	"fmt"
	"math"
	"sort"

	"github.com/monishSR/veclite/internal/vector"
)

// Centroid represents a cluster center
// Memory-efficient: only stores ID, vector stored in storage
type Centroid struct {
	ID       int    // Cluster ID
	VectorID uint64 // ID of the vector in storage that represents this centroid
}

// initializeFirstCentroid uses the first vector as the first centroid
func (i *IVFIndex) initializeFirstCentroid(id uint64, vector []float32) error {
	centroidID := i.allocateCentroidID(0)
	if err := i.storage.WriteVector(centroidID, vector); err != nil {
		return fmt.Errorf("failed to write first centroid: %w", err)
	}

	i.centroids = []Centroid{
		{ID: 0, VectorID: centroidID},
	}
	i.clusters = make(map[int][]uint64)
	i.clusters[0] = []uint64{id}
	i.vectorToCluster = make(map[uint64]int)
	i.vectorToCluster[id] = 0
	i.size = 1
	return nil
}

// addCentroidFromVector uses the vector as a new centroid
func (i *IVFIndex) addCentroidFromVector(id uint64, vector []float32) error {
	clusterID := len(i.centroids)
	centroidID := i.allocateCentroidID(clusterID)

	if err := i.storage.WriteVector(centroidID, vector); err != nil {
		return fmt.Errorf("failed to write centroid: %w", err)
	}

	i.centroids = append(i.centroids, Centroid{
		ID:       clusterID,
		VectorID: centroidID,
	})
	i.clusters[clusterID] = []uint64{id}
	i.vectorToCluster[id] = clusterID
	i.size++
	return nil
}

// findNearestCentroid finds the nearest centroid to the given vector
func (i *IVFIndex) findNearestCentroid(vec []float32) int {
	if len(i.centroids) == 0 {
		return 0
	}

	minDist := float32(math.MaxFloat32)
	nearestClusterID := 0

	// Load each centroid vector from storage and compute distance
	for clusterID := range i.centroids {
		centroidVec, err := i.getCentroidVector(clusterID)
		if err != nil {
			continue // Skip if can't load
		}
		dist := vector.L2Distance(vec, centroidVec)
		if dist < minDist {
			minDist = dist
			nearestClusterID = clusterID
		}
	}

	return nearestClusterID
}

// findNearestClusters finds the nProbe nearest centroids to the query
func (i *IVFIndex) findNearestClusters(query []float32, nProbe int) []int {
	if len(i.centroids) == 0 {
		return nil
	}

	// Calculate distances to all centroids
	type clusterDist struct {
		clusterID int
		distance  float32
	}
	distances := make([]clusterDist, 0, len(i.centroids))

	for clusterID := range i.centroids {
		centroidVec, err := i.getCentroidVector(clusterID)
		if err != nil {
			continue // Skip if can't load
		}
		dist := vector.L2Distance(query, centroidVec)
		distances = append(distances, clusterDist{
			clusterID: clusterID,
			distance:  dist,
		})
	}

	// Sort by distance (best first)
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].distance < distances[j].distance
	})

	// Return top nProbe clusters
	result := make([]int, 0, nProbe)
	for j := 0; j < nProbe && j < len(distances); j++ {
		result = append(result, distances[j].clusterID)
	}
	return result
}

// getCentroidVector loads the centroid vector from storage
func (i *IVFIndex) getCentroidVector(clusterID int) ([]float32, error) {
	if clusterID < 0 || clusterID >= len(i.centroids) {
		return nil, fmt.Errorf("invalid cluster ID: %d", clusterID)
	}
	centroid := i.centroids[clusterID]
	// Load vector from storage (cache handles caching automatically)
	return i.storage.ReadVector(centroid.VectorID)
}

// updateCentroid updates the centroid incrementally using moving average
func (i *IVFIndex) updateCentroid(clusterID int, newVector []float32) {
	if clusterID < 0 || clusterID >= len(i.centroids) {
		return
	}

	centroid := &i.centroids[clusterID]

	// Load current centroid vector
	currentVec, err := i.getCentroidVector(clusterID)
	if err != nil {
		return
	}

	// Compute new centroid as weighted average
	clusterSize := len(i.clusters[clusterID])
	if clusterSize == 0 {
		return
	}

	// New centroid = (old * (n-1) + new) / n
	newCentroid := make([]float32, i.dimension)
	for j := 0; j < i.dimension; j++ {
		newCentroid[j] = (currentVec[j]*float32(clusterSize-1) + newVector[j]) / float32(clusterSize)
	}

	// Update centroid vector in storage
	i.storage.WriteVector(centroid.VectorID, newCentroid)
}

// recomputeCentroid recomputes the centroid from all vectors in the cluster
// Used when a vector is deleted to maintain centroid accuracy
func (i *IVFIndex) recomputeCentroid(clusterID int) {
	if clusterID < 0 || clusterID >= len(i.centroids) {
		return
	}

	centroid := &i.centroids[clusterID]
	clusterVectors := i.clusters[clusterID]

	if len(clusterVectors) == 0 {
		return
	}

	// Load all vectors in cluster and compute mean
	sum := make([]float32, i.dimension)
	validCount := 0
	for _, vecID := range clusterVectors {
		// Skip centroid IDs
		const centroidIDBase = ^uint64(0)
		if vecID >= centroidIDBase-uint64(len(i.centroids)) {
			continue
		}

		vec, err := i.storage.ReadVector(vecID)
		if err != nil {
			continue // Skip if can't load
		}

		validCount++
		for j := 0; j < i.dimension; j++ {
			sum[j] += vec[j]
		}
	}

	if validCount == 0 {
		return // No valid vectors to compute centroid from
	}

	// Compute mean (centroid)
	newCentroid := make([]float32, i.dimension)
	for j := 0; j < i.dimension; j++ {
		newCentroid[j] = sum[j] / float32(validCount)
	}

	// Update centroid vector in storage
	i.storage.WriteVector(centroid.VectorID, newCentroid)
}

// allocateCentroidID allocates a unique ID for a centroid
// Uses high ID range to avoid conflicts with data vectors
func (i *IVFIndex) allocateCentroidID(clusterID int) uint64 {
	const centroidIDBase = ^uint64(0) // Max uint64
	return centroidIDBase - uint64(clusterID)
}

