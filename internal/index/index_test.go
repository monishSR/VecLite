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

