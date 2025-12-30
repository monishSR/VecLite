package ivf

import (
	"os"
	"testing"

	"github.com/monishSR/veclite/internal/storage"
)

func TestIVFIndex_InitializeFirstCentroid(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = float32(i)
	}

	err = index.initializeFirstCentroid(1, vector)
	if err != nil {
		t.Fatalf("Failed to initialize first centroid: %v", err)
	}

	if len(index.centroids) != 1 {
		t.Errorf("Expected 1 centroid, got %d", len(index.centroids))
	}

	if index.centroids[0].ID != 0 {
		t.Errorf("Expected centroid ID 0, got %d", index.centroids[0].ID)
	}

	if index.size != 1 {
		t.Errorf("Expected size 1, got %d", index.size)
	}

	if len(index.clusters[0]) != 1 {
		t.Errorf("Expected cluster 0 to have 1 vector, got %d", len(index.clusters[0]))
	}

	if index.vectorToCluster[1] != 0 {
		t.Errorf("Expected vector 1 to be in cluster 0, got %d", index.vectorToCluster[1])
	}
}

func TestIVFIndex_AddCentroidFromVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	config["NClusters"] = 5
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Initialize first centroid
	vector1 := make([]float32, 128)
	for i := range vector1 {
		vector1[i] = float32(i)
	}
	if err := index.initializeFirstCentroid(1, vector1); err != nil {
		t.Fatalf("Failed to initialize first centroid: %v", err)
	}

	// Add second centroid
	vector2 := make([]float32, 128)
	for i := range vector2 {
		vector2[i] = float32(i) + 100.0
	}
	err = index.addCentroidFromVector(2, vector2)
	if err != nil {
		t.Fatalf("Failed to add centroid from vector: %v", err)
	}

	if len(index.centroids) != 2 {
		t.Errorf("Expected 2 centroids, got %d", len(index.centroids))
	}

	if index.centroids[1].ID != 1 {
		t.Errorf("Expected centroid ID 1, got %d", index.centroids[1].ID)
	}

	if index.size != 2 {
		t.Errorf("Expected size 2, got %d", index.size)
	}

	if len(index.clusters[1]) != 1 {
		t.Errorf("Expected cluster 1 to have 1 vector, got %d", len(index.clusters[1]))
	}

	if index.vectorToCluster[2] != 1 {
		t.Errorf("Expected vector 2 to be in cluster 1, got %d", index.vectorToCluster[2])
	}
}

func TestIVFIndex_FindNearestCentroid(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	config["NClusters"] = 3
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Create 3 centroids with different vectors
	for i := 0; i < 3; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i * 100) + float32(j)
		}
		if i == 0 {
			if err := index.initializeFirstCentroid(uint64(i+1), vector); err != nil {
				t.Fatalf("Failed to initialize first centroid: %v", err)
			}
		} else {
			if err := index.addCentroidFromVector(uint64(i+1), vector); err != nil {
				t.Fatalf("Failed to add centroid: %v", err)
			}
		}
	}

	// Query vector closest to centroid 1 (values around 0-127)
	query := make([]float32, 128)
	for j := range query {
		query[j] = float32(j) + 10.0 // Close to centroid 0
	}

	nearest := index.findNearestCentroid(query)
	if nearest != 0 {
		t.Errorf("Expected nearest centroid to be 0, got %d", nearest)
	}

	// Query vector closest to centroid 2 (values around 100-227)
	query2 := make([]float32, 128)
	for j := range query2 {
		query2[j] = float32(100+j) + 10.0 // Close to centroid 1
	}

	nearest2 := index.findNearestCentroid(query2)
	if nearest2 != 1 {
		t.Errorf("Expected nearest centroid to be 1, got %d", nearest2)
	}
}

func TestIVFIndex_FindNearestCentroid_Empty(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	query := make([]float32, 128)
	nearest := index.findNearestCentroid(query)
	if nearest != 0 {
		t.Errorf("Expected nearest centroid to be 0 for empty centroids, got %d", nearest)
	}
}

func TestIVFIndex_FindNearestClusters(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	config["NClusters"] = 5
	config["NProbe"] = 2
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Create 5 centroids
	for i := 0; i < 5; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i * 100) + float32(j)
		}
		if i == 0 {
			if err := index.initializeFirstCentroid(uint64(i+1), vector); err != nil {
				t.Fatalf("Failed to initialize first centroid: %v", err)
			}
		} else {
			if err := index.addCentroidFromVector(uint64(i+1), vector); err != nil {
				t.Fatalf("Failed to add centroid: %v", err)
			}
		}
	}

	// Query vector closest to centroid 0
	query := make([]float32, 128)
	for j := range query {
		query[j] = float32(j) + 10.0
	}

	nearest := index.findNearestClusters(query, 2)
	if len(nearest) != 2 {
		t.Errorf("Expected 2 nearest clusters, got %d", len(nearest))
	}

	// First should be cluster 0 (closest)
	if nearest[0] != 0 {
		t.Errorf("Expected first nearest cluster to be 0, got %d", nearest[0])
	}
}

func TestIVFIndex_FindNearestClusters_Empty(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	query := make([]float32, 128)
	nearest := index.findNearestClusters(query, 2)
	if nearest != nil {
		t.Errorf("Expected nil for empty centroids, got %v", nearest)
	}
}

func TestIVFIndex_GetCentroidVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	originalVector := make([]float32, 128)
	for i := range originalVector {
		originalVector[i] = float32(i)
	}

	if err := index.initializeFirstCentroid(1, originalVector); err != nil {
		t.Fatalf("Failed to initialize first centroid: %v", err)
	}

	centroidVec, err := index.getCentroidVector(0)
	if err != nil {
		t.Fatalf("Failed to get centroid vector: %v", err)
	}

	if len(centroidVec) != len(originalVector) {
		t.Errorf("Vector length mismatch: expected %d, got %d", len(originalVector), len(centroidVec))
	}

	for i := range originalVector {
		if centroidVec[i] != originalVector[i] {
			t.Errorf("Vector mismatch at index %d: expected %f, got %f", i, originalVector[i], centroidVec[i])
		}
	}
}

func TestIVFIndex_GetCentroidVector_InvalidClusterID(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	_, err = index.getCentroidVector(-1)
	if err == nil {
		t.Error("Expected error for invalid cluster ID")
	}

	_, err = index.getCentroidVector(999)
	if err == nil {
		t.Error("Expected error for out of range cluster ID")
	}
}

func TestIVFIndex_UpdateCentroid(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Initialize first centroid
	vector1 := make([]float32, 128)
	for i := range vector1 {
		vector1[i] = float32(i)
	}
	if err := index.initializeFirstCentroid(1, vector1); err != nil {
		t.Fatalf("Failed to initialize first centroid: %v", err)
	}

	// Add another vector to the cluster
	vector2 := make([]float32, 128)
	for i := range vector2 {
		vector2[i] = float32(i) + 100.0
	}
	index.clusters[0] = append(index.clusters[0], 2)
	index.vectorToCluster[2] = 0
	if err := store.WriteVector(2, vector2); err != nil {
		t.Fatalf("Failed to write vector: %v", err)
	}

	// Update centroid
	index.updateCentroid(0, vector2)

	// Verify centroid was updated (should be average of vector1 and vector2)
	centroidVec, err := index.getCentroidVector(0)
	if err != nil {
		t.Fatalf("Failed to get centroid vector: %v", err)
	}

	// Centroid should be (vector1 + vector2) / 2
	for i := range vector1 {
		expected := (vector1[i] + vector2[i]) / 2.0
		if centroidVec[i] != expected {
			t.Errorf("Centroid mismatch at index %d: expected %f, got %f", i, expected, centroidVec[i])
		}
	}
}

func TestIVFIndex_RecomputeCentroid(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Initialize first centroid
	vector1 := make([]float32, 128)
	for i := range vector1 {
		vector1[i] = float32(i)
	}
	if err := index.initializeFirstCentroid(1, vector1); err != nil {
		t.Fatalf("Failed to initialize first centroid: %v", err)
	}

	// Write vector 1 to storage (it's already in the cluster, but we need it in storage)
	if err := store.WriteVector(1, vector1); err != nil {
		t.Fatalf("Failed to write vector 1: %v", err)
	}

	// Add more vectors to the cluster
	for i := uint64(2); i <= 5; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)
		}
		index.clusters[0] = append(index.clusters[0], i)
		index.vectorToCluster[i] = 0
		index.size++ // Update size
		if err := store.WriteVector(i, vector); err != nil {
			t.Fatalf("Failed to write vector: %v", err)
		}
	}

	// Recompute centroid
	index.recomputeCentroid(0)

	// Verify centroid is the mean of all vectors
	centroidVec, err := index.getCentroidVector(0)
	if err != nil {
		t.Fatalf("Failed to get centroid vector: %v", err)
	}

	// Manually compute expected mean (excluding centroid vector itself)
	expectedMean := make([]float32, 128)
	vectors := []uint64{1, 2, 3, 4, 5}
	for _, vecID := range vectors {
		vec, err := store.ReadVector(vecID)
		if err != nil {
			continue
		}
		for j := range expectedMean {
			expectedMean[j] += vec[j]
		}
	}
	for j := range expectedMean {
		expectedMean[j] /= float32(len(vectors))
	}

	// Allow small floating point differences
	for j := range expectedMean {
		diff := centroidVec[j] - expectedMean[j]
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Errorf("Centroid mismatch at index %d: expected %f, got %f", j, expectedMean[j], centroidVec[j])
		}
	}
}

func TestIVFIndex_AllocateCentroidID(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	index, err := NewIVFIndex(128, config, store)
	if err != nil {
		t.Fatalf("Failed to create IVF index: %v", err)
	}

	// Allocate IDs for different clusters
	id0 := index.allocateCentroidID(0)
	id1 := index.allocateCentroidID(1)
	id2 := index.allocateCentroidID(2)

	// IDs should be in high range and unique
	if id0 == id1 || id0 == id2 || id1 == id2 {
		t.Error("Centroid IDs should be unique")
	}

	// IDs should be very large (near max uint64)
	const maxUint64 = ^uint64(0)
	if id0 < maxUint64-100 {
		t.Errorf("Centroid ID should be in high range, got %d", id0)
	}

	// IDs should decrease as cluster ID increases
	if id0 <= id1 || id1 <= id2 {
		t.Error("Centroid IDs should decrease as cluster ID increases")
	}
}

