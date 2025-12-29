package index

import (
	"os"
	"testing"

	"github.com/monishSR/veclite/internal/index/hnsw"
	"github.com/monishSR/veclite/internal/storage"
)

func createTempFile(t *testing.T) string {
	tmpFile, err := os.CreateTemp("", "veclite_index_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}

func TestNewIndex_UnknownType(t *testing.T) {
	// Test unknown index type
	_, err := NewIndex(IndexType("unknown"), 128, nil, nil)
	if err == nil {
		t.Error("Expected error for unknown index type")
	}
}

func TestNewIndex_Flat(t *testing.T) {
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

	// Test creating new Flat index
	idx, err := NewIndex(IndexTypeFlat, 128, nil, store)
	if err != nil {
		t.Fatalf("Failed to create Flat index: %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}

	// Test Flat index without storage
	idx2, err := NewIndex(IndexTypeFlat, 128, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create Flat index without storage: %v", err)
	}
	if idx2 == nil {
		t.Fatal("NewIndex returned nil")
	}
}

func TestNewIndex_HNSW_New(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + ".graph")

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	// Test creating new HNSW index (no graph file exists)
	idx, err := NewIndex(IndexTypeHNSW, 128, config, store)
	if err != nil {
		t.Fatalf("Failed to create HNSW index: %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}
}

func TestNewIndex_HNSW_Open(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + ".graph")

	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	// Create HNSW index and save graph
	idx1, err := NewIndex(IndexTypeHNSW, 128, config, store)
	if err != nil {
		t.Fatalf("Failed to create HNSW index: %v", err)
	}

	// Insert a vector
	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = 1.0 + float32(i)*0.001
	}
	if err := idx1.Insert(1, vector); err != nil {
		t.Fatalf("Failed to insert vector: %v", err)
	}

	// Save graph
	if hnswIdx, ok := idx1.(*hnsw.HNSWIndex); ok {
		if err := hnswIdx.SaveGraph(); err != nil {
			t.Fatalf("Failed to save graph: %v", err)
		}
	}

	// Close storage and reopen
	store.Close()

	store2, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store2.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store2.Close()

	// Test opening existing HNSW index (graph file exists)
	idx2, err := NewIndex(IndexTypeHNSW, 128, config, store2)
	if err != nil {
		t.Fatalf("Failed to open HNSW index: %v", err)
	}
	if idx2 == nil {
		t.Fatal("NewIndex returned nil")
	}

	// Verify vector can be read
	vec, err := idx2.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector: %v", err)
	}
	if len(vec) != 128 {
		t.Errorf("Expected vector dimension 128, got %d", len(vec))
	}
}

func TestNewIndex_HNSW_NoStorage(t *testing.T) {
	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	// Test HNSW without storage (should create new index)
	idx, err := NewIndex(IndexTypeHNSW, 128, config, nil)
	if err != nil {
		t.Fatalf("Failed to create HNSW index without storage: %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}
}

func TestNewIndex_Flat_WithExistingData(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Create storage and write some vectors
	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}

	// Write some vectors
	for i := uint64(1); i <= 3; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = float32(i) + float32(j)*0.001
		}
		if err := store.WriteVector(i, vector); err != nil {
			t.Fatalf("Failed to write vector %d: %v", i, err)
		}
	}
	store.Close()

	// Reopen storage
	store2, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store2.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store2.Close()

	// Test opening existing Flat index (should call OpenFlatIndex)
	idx, err := NewIndex(IndexTypeFlat, 128, nil, store2)
	if err != nil {
		t.Fatalf("Failed to open Flat index: %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}

	// Verify vectors were loaded
	if idx.Size() != 3 {
		t.Errorf("Expected size 3, got %d", idx.Size())
	}

	// Verify we can read a vector
	vec, err := idx.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector 1: %v", err)
	}
	if len(vec) != 128 {
		t.Errorf("Expected vector dimension 128, got %d", len(vec))
	}
}

func TestNewIndex_Flat_OpenFlatIndexError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Create storage but don't open it
	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	// Don't open storage - this will cause OpenFlatIndex to fail

	// Test that NewIndex handles OpenFlatIndex error
	_, err = NewIndex(IndexTypeFlat, 128, nil, store)
	if err == nil {
		t.Error("Expected error when OpenFlatIndex fails (storage not open)")
	}
}

func TestNewIndex_IVF(t *testing.T) {
	// IVF is a placeholder that returns successfully but operations will fail
	config := make(map[string]any)
	idx, err := NewIndex(IndexTypeIVF, 128, config, nil)
	if err != nil {
		t.Fatalf("NewIndex should succeed for IVF (placeholder): %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}

	// Verify it's an IVF index by checking that operations fail (not implemented)
	vector := make([]float32, 128)
	err = idx.Insert(1, vector)
	if err == nil {
		t.Error("Expected error for IVF Insert (not implemented)")
	}
}

func TestNewIndex_IVF_WithStorage(t *testing.T) {
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

	// IVF is a placeholder that returns successfully but operations will fail
	config := make(map[string]any)
	idx, err := NewIndex(IndexTypeIVF, 128, config, store)
	if err != nil {
		t.Fatalf("NewIndex should succeed for IVF (placeholder): %v", err)
	}
	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}

	// Verify it's an IVF index by checking that operations fail (not implemented)
	vector := make([]float32, 128)
	err = idx.Insert(1, vector)
	if err == nil {
		t.Error("Expected error for IVF Insert (not implemented)")
	}
}

func TestNewIndex_HNSW_OpenHNSWIndexError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + ".graph")

	// Create a fake graph file to trigger OpenHNSWIndex path
	graphFile := tmpFile + ".graph"
	file, err := os.Create(graphFile)
	if err != nil {
		t.Fatalf("Failed to create graph file: %v", err)
	}
	file.Close()

	// Create storage but don't open it (this will cause OpenHNSWIndex to fail)
	store, err := storage.NewStorage(tmpFile, 128, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	// Don't open storage - this will cause OpenHNSWIndex to fail

	config := make(map[string]any)
	config["M"] = 16
	config["EfConstruction"] = 200
	config["EfSearch"] = 50

	// Test that NewIndex handles OpenHNSWIndex error
	_, err = NewIndex(IndexTypeHNSW, 128, config, store)
	if err == nil {
		t.Error("Expected error when OpenHNSWIndex fails (storage not open)")
	}
}

