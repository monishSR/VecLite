package hnsw

import (
	"os"
	"testing"

	"github.com/monishSR/veclite/internal/index/types"
	"github.com/monishSR/veclite/internal/storage"
)

func createTempFile(t *testing.T) string {
	tmpFile, err := os.CreateTemp("", "veclite_hnsw_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}

func createTestHNSW(t *testing.T) (*HNSWIndex, func()) {
	tmpFile := createTempFile(t)
	graphFile := tmpFile + ".graph"

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}

	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	index, err := NewHNSWIndex(128, config, store)
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create HNSW index: %v", err)
	}

	cleanup := func() {
		index.Clear()
		store.Close()
		os.Remove(tmpFile)
		os.Remove(graphFile)
	}

	return index, cleanup
}

func TestHNSWIndex_Insert_FirstNode(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = float32(i)
	}

	err := index.Insert(1, vector)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if index.Size() != 1 {
		t.Errorf("Expected size 1, got %d", index.Size())
	}

	if index.entryPoint == 0 {
		t.Error("Entry point should be set after first insert")
	}

	if index.maxLevel < 0 {
		t.Error("Max level should be >= 0 after first insert")
	}
}

func TestHNSWIndex_Insert_Multiple(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert multiple vectors
	for i := uint64(1); i <= 10; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	if index.Size() != 10 {
		t.Errorf("Expected size 10, got %d", index.Size())
	}
}

func TestHNSWIndex_Insert_DimensionMismatch(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	wrongVector := make([]float32, 64) // Wrong dimension
	err := index.Insert(1, wrongVector)
	if err == nil {
		t.Error("Expected error for dimension mismatch")
	}
	if err != types.ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got %v", err)
	}
}

func TestHNSWIndex_Insert_UpdateExisting(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	vector1 := make([]float32, 128)
	for i := range vector1 {
		vector1[i] = float32(i)
	}

	// First insert
	if err := index.Insert(1, vector1); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Update with new vector
	vector2 := make([]float32, 128)
	for i := range vector2 {
		vector2[i] = float32(i) * 2.0
	}

	if err := index.Insert(1, vector2); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Size should remain 1
	if index.Size() != 1 {
		t.Errorf("Expected size 1 after update, got %d", index.Size())
	}
}

func TestHNSWIndex_Search_EmptyIndex(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	query := make([]float32, 128)
	results, err := index.Search(query, 5)
	if err != nil {
		t.Fatalf("Search should not error on empty index: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results from empty index, got %d", len(results))
	}
}

func TestHNSWIndex_Search_Basic(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert vectors
	vectors := make(map[uint64][]float32)
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		vectors[i] = vector
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Search using first vector as query (should find itself)
	query := vectors[1]
	results, err := index.Search(query, 3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected at least one result")
	}

	// First result should be the query vector itself (distance = 0)
	if results[0].ID != 1 {
		t.Errorf("Expected first result to be ID 1, got %d", results[0].ID)
	}
	if results[0].Distance > 0.001 {
		t.Errorf("Expected distance ~0 for same vector, got %f", results[0].Distance)
	}
}

func TestHNSWIndex_Search_DifferentK(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert 10 vectors
	for i := uint64(1); i <= 10; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = 5.0 + float32(j)*0.001
	}

	// Search with k=3
	results3, err := index.Search(query, 3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results3) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results3))
	}

	// Search with k=10 (more than available)
	results10, err := index.Search(query, 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results10) > 10 {
		t.Errorf("Expected at most 10 results, got %d", len(results10))
	}
}

func TestHNSWIndex_Search_DimensionMismatch(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert a vector
	vector := make([]float32, 128)
	if err := index.Insert(1, vector); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Search with wrong dimension
	wrongQuery := make([]float32, 64)
	_, err := index.Search(wrongQuery, 5)
	if err == nil {
		t.Error("Expected error for dimension mismatch")
	}
	if err != types.ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got %v", err)
	}
}

func TestHNSWIndex_Search_InvalidK(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	vector := make([]float32, 128)
	if err := index.Insert(1, vector); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	query := make([]float32, 128)
	_, err := index.Search(query, 0)
	if err == nil {
		t.Error("Expected error for k=0")
	}
	if err != types.ErrInvalidK {
		t.Errorf("Expected ErrInvalidK, got %v", err)
	}
}

func TestHNSWIndex_ReadVector(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	originalVector := make([]float32, 128)
	for i := range originalVector {
		originalVector[i] = float32(i)
	}

	if err := index.Insert(1, originalVector); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	readVector, err := index.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector: %v", err)
	}

	if len(readVector) != len(originalVector) {
		t.Errorf("Vector length mismatch: expected %d, got %d", len(originalVector), len(readVector))
	}

	for i := range originalVector {
		if readVector[i] != originalVector[i] {
			t.Errorf("Vector mismatch at index %d: expected %f, got %f", i, originalVector[i], readVector[i])
		}
	}
}

func TestHNSWIndex_ReadVector_NotFound(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	_, err := index.ReadVector(999)
	if err == nil {
		t.Error("Expected error when reading non-existent vector")
	}
}

func TestHNSWIndex_Size(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	if index.Size() != 0 {
		t.Errorf("Expected size 0 for empty index, got %d", index.Size())
	}

	// Insert vectors
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
		if index.Size() != int(i) {
			t.Errorf("Expected size %d after inserting %d vectors, got %d", i, i, index.Size())
		}
	}
}

func TestHNSWIndex_Delete(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert vectors
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i)
		}
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	if index.Size() != 5 {
		t.Errorf("Expected size 5, got %d", index.Size())
	}

	// Delete a vector
	if err := index.Delete(3); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if index.Size() != 4 {
		t.Errorf("Expected size 4 after delete, got %d", index.Size())
	}

	// Verify it's deleted
	_, err := index.ReadVector(3)
	if err == nil {
		t.Error("Expected error when reading deleted vector")
	}
}

func TestHNSWIndex_Delete_NonExistent(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Delete non-existent vector should not error
	if err := index.Delete(999); err != nil {
		t.Errorf("Delete should not error for non-existent vector: %v", err)
	}
}

func TestHNSWIndex_Clear(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert vectors
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	if index.Size() != 5 {
		t.Errorf("Expected size 5, got %d", index.Size())
	}

	// Clear
	if err := index.Clear(); err != nil {
		t.Fatalf("Failed to clear: %v", err)
	}

	if index.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", index.Size())
	}

	if index.entryPoint != 0 {
		t.Error("Entry point should be reset after clear")
	}

	if index.maxLevel != -1 {
		t.Errorf("Max level should be -1 after clear, got %d", index.maxLevel)
	}
}

func TestHNSWIndex_SaveGraph_LoadGraph(t *testing.T) {
	tmpFile := createTempFile(t)
	graphFile := tmpFile + ".graph"
	defer os.Remove(tmpFile)
	defer os.Remove(graphFile)

	// Create and populate index
	store1, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store1.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}

	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	index1, err := NewHNSWIndex(128, config, store1)
	if err != nil {
		store1.Close()
		t.Fatalf("Failed to create HNSW index: %v", err)
	}

	// Insert vectors
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		if err := index1.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Save graph
	if err := index1.SaveGraph(); err != nil {
		t.Fatalf("Failed to save graph: %v", err)
	}
	store1.Close()

	// Load graph in new index
	store2, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store2.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store2.Close()

	index2, err := OpenHNSWIndex(store2)
	if err != nil {
		t.Fatalf("Failed to open HNSW index: %v", err)
	}

	// Verify size
	if index2.Size() != 5 {
		t.Errorf("Expected size 5 after load, got %d", index2.Size())
	}

	// Verify we can read vectors
	for i := uint64(1); i <= 5; i++ {
		vec, err := index2.ReadVector(i)
		if err != nil {
			t.Fatalf("Failed to read vector %d: %v", i, err)
		}
		if len(vec) != 128 {
			t.Errorf("Vector %d has wrong dimension: expected 128, got %d", i, len(vec))
		}
	}

	// Verify we can search
	query := make([]float32, 128)
	for j := range query {
		query[j] = 3.0 + float32(j)*0.001
	}
	results, err := index2.Search(query, 3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Error("Expected search results after loading graph")
	}
}

func TestHNSWIndex_MultipleLevels(t *testing.T) {
	index, cleanup := createTestHNSW(t)
	defer cleanup()

	// Insert many vectors to increase chance of multiple levels
	for i := uint64(1); i <= 100; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		if err := index.Insert(i, vector); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Verify structure
	if index.Size() != 100 {
		t.Errorf("Expected size 100, got %d", index.Size())
	}

	if index.entryPoint == 0 {
		t.Error("Entry point should be set")
	}

	// Search should work
	query := make([]float32, 128)
	for j := range query {
		query[j] = 50.0 + float32(j)*0.001
	}
	results, err := index.Search(query, 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) == 0 {
		t.Error("Expected search results")
	}

	// Verify results are sorted by distance
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("Results not sorted: result %d has distance %f < result %d distance %f",
				i, results[i].Distance, i-1, results[i-1].Distance)
		}
	}
}
