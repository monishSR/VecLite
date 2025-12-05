package storage

import (
	"os"
	"testing"
)

func TestNewStorage(t *testing.T) {
	s, err := NewStorage("test.db")
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	if s == nil {
		t.Fatal("NewStorage returned nil")
	}
	if s.filePath != "test.db" {
		t.Errorf("Expected filePath 'test.db', got '%s'", s.filePath)
	}
	if s.index == nil {
		t.Error("Index map not initialized")
	}
}

func TestOpen_NewFile(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	err = s.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if s.file == nil {
		t.Error("File not opened")
	}

	s.Close()
}

func TestWriteVector_ReadVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write a vector
	id := uint64(1)
	vector := []float32{1.0, 2.0, 3.0, 4.0}

	if err := s.WriteVector(id, vector); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Verify it's in the index
	offset, exists := s.index[id]
	if !exists {
		t.Error("Vector not in index after write")
	}
	if offset < 0 {
		t.Errorf("Invalid offset: %d", offset)
	}

	// Read it back
	readVector, err := s.ReadVector(id)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	if len(readVector) != len(vector) {
		t.Errorf("Vector length mismatch: expected %d, got %d", len(vector), len(readVector))
	}

	for i := range vector {
		if readVector[i] != vector[i] {
			t.Errorf("Vector mismatch at index %d: expected %f, got %f", i, vector[i], readVector[i])
		}
	}
}

func TestWriteVector_Multiple(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write multiple vectors
	vectors := map[uint64][]float32{
		1: {1.0, 2.0, 3.0},
		2: {4.0, 5.0, 6.0},
		3: {7.0, 8.0, 9.0},
	}

	for id, vec := range vectors {
		if err := s.WriteVector(id, vec); err != nil {
			t.Fatalf("WriteVector failed for ID %d: %v", id, err)
		}
	}

	// Verify all are in index
	if len(s.index) != len(vectors) {
		t.Errorf("Index size mismatch: expected %d, got %d", len(vectors), len(s.index))
	}

	// Read all back
	for id, expectedVec := range vectors {
		readVec, err := s.ReadVector(id)
		if err != nil {
			t.Fatalf("ReadVector failed for ID %d: %v", id, err)
		}

		if len(readVec) != len(expectedVec) {
			t.Errorf("Vector length mismatch for ID %d: expected %d, got %d", id, len(expectedVec), len(readVec))
		}

		for i := range expectedVec {
			if readVec[i] != expectedVec[i] {
				t.Errorf("Vector mismatch for ID %d at index %d: expected %f, got %f", id, i, expectedVec[i], readVec[i])
			}
		}
	}
}

func TestReadVector_NotFound(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Try to read non-existent vector
	_, err = s.ReadVector(999)
	if err == nil {
		t.Error("Expected error when reading non-existent vector")
	}
}

func TestDeleteVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write a vector
	id := uint64(1)
	vector := []float32{1.0, 2.0, 3.0}

	if err := s.WriteVector(id, vector); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Verify it exists
	_, err = s.ReadVector(id)
	if err != nil {
		t.Fatalf("ReadVector failed before delete: %v", err)
	}

	// Delete it
	if err := s.DeleteVector(id); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Verify it's removed from index
	if _, exists := s.index[id]; exists {
		t.Error("Vector still in index after delete")
	}

	// Verify it can't be read
	_, err = s.ReadVector(id)
	if err == nil {
		t.Error("Expected error when reading deleted vector")
	}
}

func TestDeleteVector_NonExistent(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Delete non-existent vector (should not error)
	if err := s.DeleteVector(999); err != nil {
		t.Errorf("DeleteVector should not error for non-existent vector: %v", err)
	}
}

func TestReadAllVectors(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write multiple vectors
	vectors := map[uint64][]float32{
		1: {1.0, 2.0},
		2: {3.0, 4.0},
		3: {5.0, 6.0},
	}

	for id, vec := range vectors {
		if err := s.WriteVector(id, vec); err != nil {
			t.Fatalf("WriteVector failed for ID %d: %v", id, err)
		}
	}

	// Read all vectors
	allVectors, err := s.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}

	if len(allVectors) != len(vectors) {
		t.Errorf("Expected %d vectors, got %d", len(vectors), len(allVectors))
	}

	// Verify all vectors match
	for id, expectedVec := range vectors {
		readVec, exists := allVectors[id]
		if !exists {
			t.Errorf("Vector %d not found in ReadAllVectors", id)
			continue
		}

		if len(readVec) != len(expectedVec) {
			t.Errorf("Vector %d length mismatch: expected %d, got %d", id, len(expectedVec), len(readVec))
			continue
		}

		for i := range expectedVec {
			if readVec[i] != expectedVec[i] {
				t.Errorf("Vector %d mismatch at index %d: expected %f, got %f", id, i, expectedVec[i], readVec[i])
			}
		}
	}
}

func TestReadAllVectors_SkipsTombstones(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Delete one
	if err := s.DeleteVector(2); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Read all - should skip deleted vector
	allVectors, err := s.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}

	if len(allVectors) != 2 {
		t.Errorf("Expected 2 vectors, got %d", len(allVectors))
	}

	if _, exists := allVectors[2]; exists {
		t.Error("Deleted vector should not be in ReadAllVectors result")
	}

	if _, exists := allVectors[1]; !exists {
		t.Error("Vector 1 should be in result")
	}

	if _, exists := allVectors[3]; !exists {
		t.Error("Vector 3 should be in result")
	}
}

func TestIndexPersistence(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// First session: write vectors
	s1, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	vectors := map[uint64][]float32{
		1: {1.0, 2.0, 3.0},
		2: {4.0, 5.0, 6.0},
		3: {7.0, 8.0, 9.0},
	}

	for id, vec := range vectors {
		if err := s1.WriteVector(id, vec); err != nil {
			t.Fatalf("WriteVector failed for ID %d: %v", id, err)
		}
	}

	s1.Close()

	// Second session: open and verify index was loaded
	s2, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s2.Close()

	// Verify index was loaded
	if len(s2.index) != len(vectors) {
		t.Errorf("Index size mismatch: expected %d, got %d", len(vectors), len(s2.index))
	}

	// Verify all vectors can be read
	for id, expectedVec := range vectors {
		readVec, err := s2.ReadVector(id)
		if err != nil {
			t.Fatalf("ReadVector failed for ID %d: %v", id, err)
		}

		if len(readVec) != len(expectedVec) {
			t.Errorf("Vector %d length mismatch: expected %d, got %d", id, len(expectedVec), len(readVec))
		}
	}
}

func TestCompaction(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Sync to save index
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Get file size before deletion (includes index)
	fileInfo1, err := s.file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	sizeBeforeDelete := fileInfo1.Size()

	// Delete one vector
	if err := s.DeleteVector(2); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Get file size after deletion (should be same - tombstone, no index saved yet)
	fileInfo2, err := s.file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	sizeAfterDelete := fileInfo2.Size()

	if sizeAfterDelete != sizeBeforeDelete {
		t.Errorf("File size should not change after delete (tombstone): before %d, after %d", sizeBeforeDelete, sizeAfterDelete)
	}

	// Verify deleted vector is marked but still in file
	allVectorsBefore, err := s.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}
	if len(allVectorsBefore) != 2 {
		t.Errorf("Expected 2 active vectors before compaction, got %d", len(allVectorsBefore))
	}

	// Close (triggers compaction and saves index)
	s.Close()

	// Reopen and verify compaction worked
	s2, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s2.Close()

	// Verify deleted vector is gone
	allVectors, err := s2.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}

	if len(allVectors) != 2 {
		t.Errorf("Expected 2 vectors after compaction, got %d", len(allVectors))
	}

	if _, exists := allVectors[2]; exists {
		t.Error("Deleted vector should not exist after compaction")
	}

	// Verify remaining vectors are correct
	if _, exists := allVectors[1]; !exists {
		t.Error("Vector 1 should exist after compaction")
	}
	if _, exists := allVectors[3]; !exists {
		t.Error("Vector 3 should exist after compaction")
	}
}

func TestSync(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Sync should not error
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestOpen_EmptyFile(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Open empty file should succeed (rebuilds empty index)
	if err := s.Open(); err != nil {
		t.Fatalf("Open failed on empty file: %v", err)
	}
	defer s.Close()

	if len(s.index) != 0 {
		t.Errorf("Expected empty index, got %d entries", len(s.index))
	}
}

func TestWriteVector_WithoutOpen(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Try to write without opening
	err = s.WriteVector(1, []float32{1.0, 2.0})
	if err == nil {
		t.Error("Expected error when writing without opening file")
	}
}

func TestReadVector_WithoutOpen(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Try to read without opening
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading without opening file")
	}
}

func TestClear(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write some vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Verify vectors exist
	if len(s.index) != 3 {
		t.Errorf("Expected 3 vectors in index, got %d", len(s.index))
	}

	// Clear all vectors
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify index is empty
	if len(s.index) != 0 {
		t.Errorf("Expected empty index after Clear, got %d entries", len(s.index))
	}

	// Verify file is empty
	fileInfo, err := s.file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if fileInfo.Size() != 0 {
		t.Errorf("Expected file size 0 after Clear, got %d", fileInfo.Size())
	}

	// Verify vectors can't be read
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading vector after Clear")
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
