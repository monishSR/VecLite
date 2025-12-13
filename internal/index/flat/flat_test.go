package flat

import (
	"os"
	"testing"

	"github.com/monishSR/veclite/internal/index/types"
	"github.com/monishSR/veclite/internal/storage"
)

func TestFlatIndex_Insert(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = float32(i)
	}

	err = index.Insert(1, vector)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if index.Size() != 1 {
		t.Errorf("Expected size 1, got %d", index.Size())
	}
}

func TestFlatIndex_Search(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	index := NewFlatIndex(3, store)

	// Insert some vectors
	index.Insert(1, []float32{1.0, 0.0, 0.0})
	index.Insert(2, []float32{0.0, 1.0, 0.0})
	index.Insert(3, []float32{0.0, 0.0, 1.0})

	// Search for vector closest to [1, 0, 0]
	results, err := index.Search([]float32{1.0, 0.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// First result should be ID 1 with distance 0
	if results[0].ID != 1 {
		t.Errorf("Expected ID 1, got %d", results[0].ID)
	}

	if results[0].Distance > 0.001 {
		t.Errorf("Expected distance ~0, got %f", results[0].Distance)
	}
}

func TestFlatIndex_Delete(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	vector := make([]float32, 128)
	index.Insert(1, vector)

	if index.Size() != 1 {
		t.Errorf("Expected size 1, got %d", index.Size())
	}

	err = index.Delete(1)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if index.Size() != 0 {
		t.Errorf("Expected size 0, got %d", index.Size())
	}
}

func TestFlatIndex_Delete_NonExistent(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	// Deleting non-existent ID should not error
	err = index.Delete(999)
	if err != nil {
		t.Errorf("Delete should not error for non-existent ID, got: %v", err)
	}
}

func TestFlatIndex_ReadVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	index := NewFlatIndex(3, store)

	originalVec := []float32{1.0, 2.0, 3.0}
	err = index.Insert(1, originalVec)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Read the vector
	readVec, err := index.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector: %v", err)
	}

	if len(readVec) != len(originalVec) {
		t.Errorf("Expected vector length %d, got %d", len(originalVec), len(readVec))
	}

	for i := range originalVec {
		if readVec[i] != originalVec[i] {
			t.Errorf("Vector mismatch at index %d: expected %f, got %f", i, originalVec[i], readVec[i])
		}
	}

	// Verify it's a copy (modifying readVec shouldn't affect stored vector)
	readVec[0] = 999.0
	readVec2, err2 := index.ReadVector(1)
	if err2 != nil {
		t.Fatalf("Failed to read vector second time: %v", err2)
	}
	if readVec2[0] == 999.0 {
		t.Error("ReadVector should return a copy, not a reference")
	}
}

func TestFlatIndex_ReadVector_NotFound(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	_, err2 := index.ReadVector(999)
	if err2 == nil {
		t.Error("Expected error when reading non-existent vector")
	}
}

func TestFlatIndex_Size(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	if index.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", index.Size())
	}

	// Insert multiple vectors
	for i := uint64(1); i <= 5; i++ {
		vector := make([]float32, 128)
		index.Insert(i, vector)
	}

	if index.Size() != 5 {
		t.Errorf("Expected size 5, got %d", index.Size())
	}

	// Delete one
	err = index.Delete(3)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if index.Size() != 4 {
		t.Errorf("Expected size 4 after delete, got %d", index.Size())
	}
}

func TestFlatIndex_Clear(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	// Insert some vectors
	for i := uint64(1); i <= 3; i++ {
		vector := make([]float32, 128)
		index.Insert(i, vector)
	}

	if index.Size() != 3 {
		t.Errorf("Expected size 3, got %d", index.Size())
	}

	// Clear all
	err = index.Clear()
	if err != nil {
		t.Fatalf("Failed to clear: %v", err)
	}

	if index.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", index.Size())
	}

	// Verify vectors are gone
	_, err2 := index.ReadVector(1)
	if err2 == nil {
		t.Error("Vector should not exist after clear")
	}
}

func TestFlatIndex_Insert_DimensionMismatch(t *testing.T) {
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

	index := NewFlatIndex(128, store)

	err = index.Insert(1, make([]float32, 64)) // Wrong dimension
	if err == nil {
		t.Error("Expected error for dimension mismatch")
	}
	if err != types.ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got: %v", err)
	}
}

func TestFlatIndex_Search_DimensionMismatch(t *testing.T) {
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

	index := NewFlatIndex(3, store)

	index.Insert(1, []float32{1.0, 2.0, 3.0})

	_, err2 := index.Search([]float32{1.0, 2.0}, 1) // Wrong dimension
	if err2 == nil {
		t.Error("Expected error for dimension mismatch")
	}
	if err2 != types.ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got: %v", err2)
	}
}

func TestFlatIndex_Search_InvalidK(t *testing.T) {
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

	index := NewFlatIndex(3, store)

	index.Insert(1, []float32{1.0, 2.0, 3.0})

	_, err2 := index.Search([]float32{1.0, 2.0, 3.0}, 0)
	if err2 == nil {
		t.Error("Expected error for k <= 0")
	}
	if err2 != types.ErrInvalidK {
		t.Errorf("Expected ErrInvalidK, got: %v", err2)
	}

	_, err3 := index.Search([]float32{1.0, 2.0, 3.0}, -1)
	if err3 == nil {
		t.Error("Expected error for k <= 0")
	}
}

func TestFlatIndex_Search_EmptyIndex(t *testing.T) {
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

	index := NewFlatIndex(3, store)

	results, err := index.Search([]float32{1.0, 2.0, 3.0}, 5)
	if err != nil {
		t.Fatalf("Search should not error on empty index: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results from empty index, got %d", len(results))
	}
}

func TestFlatIndex_Search_KGreaterThanAvailable(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	index := NewFlatIndex(3, store)

	index.Insert(1, []float32{1.0, 0.0, 0.0})
	index.Insert(2, []float32{0.0, 1.0, 0.0})

	// Request more results than available
	results, err := index.Search([]float32{1.0, 0.0, 0.0}, 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Should return only available results
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestFlatIndex_Search_VectorField(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	store, err := storage.NewStorage(tmpFile, 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store.Close()

	index := NewFlatIndex(3, store)

	vec1 := []float32{1.0, 0.0, 0.0}
	vec2 := []float32{0.0, 1.0, 0.0}
	index.Insert(1, vec1)
	index.Insert(2, vec2)

	results, err := index.Search([]float32{1.0, 0.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Verify Vector field is populated
	if results[0].Vector == nil {
		t.Error("Expected Vector field to be populated in search results")
	}

	if len(results[0].Vector) != 3 {
		t.Errorf("Expected vector length 3, got %d", len(results[0].Vector))
	}

	// Verify first result has correct vector
	for i := range vec1 {
		if results[0].Vector[i] != vec1[i] {
			t.Errorf("Vector mismatch at index %d: expected %f, got %f", i, vec1[i], results[0].Vector[i])
		}
	}
}

func TestFlatIndex_OpenFlatIndex(t *testing.T) {
	// Create temporary storage file
	tmpFile, err := os.CreateTemp("", "veclite_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Create storage and write some vectors
	store, err := storage.NewStorage(tmpFile.Name(), 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}

	// Write vectors
	store.WriteVector(1, []float32{1.0, 2.0, 3.0})
	store.WriteVector(2, []float32{4.0, 5.0, 6.0})
	store.Close()

	// Reopen storage
	store2, err := storage.NewStorage(tmpFile.Name(), 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store2.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store2.Close()

	// Open flat index from storage
	index, err := OpenFlatIndex(3, store2)
	if err != nil {
		t.Fatalf("Failed to open flat index: %v", err)
	}

	// Verify vectors were loaded
	if index.Size() != 2 {
		t.Errorf("Expected size 2, got %d", index.Size())
	}

	// Verify we can read vectors
	vec1, err := index.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector 1: %v", err)
	}
	if len(vec1) != 3 || vec1[0] != 1.0 || vec1[1] != 2.0 || vec1[2] != 3.0 {
		t.Errorf("Vector 1 mismatch: got %v", vec1)
	}
}

func TestFlatIndex_OpenFlatIndex_NoStorage(t *testing.T) {
	_, err := OpenFlatIndex(3, nil)
	if err == nil {
		t.Error("Expected error when opening with nil storage")
	}
}

func TestFlatIndex_OpenFlatIndex_DimensionMismatch(t *testing.T) {
	// Create temporary storage file
	tmpFile, err := os.CreateTemp("", "veclite_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Create storage and write vector with dimension 2
	store, err := storage.NewStorage(tmpFile.Name(), 2, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}

	store.WriteVector(1, []float32{1.0, 2.0}) // 2D vector
	store.Close()

	// Try to open with dimension 3 (should fail due to dimension mismatch)
	store2, err := storage.NewStorage(tmpFile.Name(), 3, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if err := store2.Open(); err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	defer store2.Close()

	_, err2 := OpenFlatIndex(3, store2)
	if err2 == nil {
		t.Error("Expected error for dimension mismatch")
	}
}

// Helper function to create a temporary file
func createTempFile(t *testing.T) string {
	tmpFile, err := os.CreateTemp("", "veclite_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}
