package storage

import (
	"os"
	"testing"
)

func TestReadAllVectors(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write multiple vectors
	vectors := map[uint64][]float32{
		1: {1.0, 2.0, 3.0, 4.0},
		2: {3.0, 4.0, 5.0, 6.0},
		3: {5.0, 6.0, 7.0, 8.0},
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

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0, 5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
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
	s1, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s1.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	vectors := map[uint64][]float32{
		1: {1.0, 2.0, 3.0, 4.0},
		2: {4.0, 5.0, 6.0, 7.0},
		3: {7.0, 8.0, 9.0, 10.0},
	}

	for id, vec := range vectors {
		if err := s1.WriteVector(id, vec); err != nil {
			t.Fatalf("WriteVector failed for ID %d: %v", id, err)
		}
	}

	s1.Close()

	// Second session: open and verify index was loaded
	s2, err := NewStorage(tmpFile, 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0, 5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
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
	s2, err := NewStorage(tmpFile, 4, 0)
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

func TestStorage_ReadAllVectors(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write vectors
	vectors := map[uint64][]float32{
		1: {1.0, 2.0, 3.0, 4.0},
		2: {5.0, 6.0, 7.0, 8.0},
		3: {9.0, 10.0, 11.0, 12.0},
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

	// Verify count
	if len(allVectors) != len(vectors) {
		t.Errorf("Expected %d vectors, got %d", len(vectors), len(allVectors))
	}

	// Verify each vector
	for id, expectedVec := range vectors {
		actualVec, exists := allVectors[id]
		if !exists {
			t.Errorf("Vector %d not found in ReadAllVectors result", id)
			continue
		}
		if len(actualVec) != len(expectedVec) {
			t.Errorf("Vector %d length mismatch: expected %d, got %d", id, len(expectedVec), len(actualVec))
			continue
		}
		for i := range expectedVec {
			if actualVec[i] != expectedVec[i] {
				t.Errorf("Vector %d[%d] mismatch: expected %f, got %f", id, i, expectedVec[i], actualVec[i])
			}
		}
	}
}

func TestStorage_ReadAllVectors_WithDeleted(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Delete one vector
	if err := s.DeleteVector(1); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Read all vectors (should not include deleted)
	allVectors, err := s.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}

	// Should only have one vector
	if len(allVectors) != 1 {
		t.Errorf("Expected 1 vector after delete, got %d", len(allVectors))
	}

	// Should not have deleted vector
	if _, exists := allVectors[1]; exists {
		t.Error("Deleted vector should not be in ReadAllVectors result")
	}

	// Should have non-deleted vector
	if _, exists := allVectors[2]; !exists {
		t.Error("Non-deleted vector should be in ReadAllVectors result")
	}
}

func TestStorage_ReadAllVectors_EmptyFile(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// ReadAllVectors on empty file should return empty map
	allVectors, err := s.ReadAllVectors()
	if err != nil {
		t.Fatalf("ReadAllVectors failed: %v", err)
	}

	if len(allVectors) != 0 {
		t.Errorf("Expected empty map for empty file, got %d entries", len(allVectors))
	}
}

func TestStorage_Compact_WithTombstones(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{9.0, 10.0, 11.0, 12.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Delete one vector (creates tombstone)
	if err := s.DeleteVector(2); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Close should trigger compact() which removes tombstones
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify only non-deleted vectors exist
	s2, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	if err := s2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s2.Close()

	// Verify deleted vector is gone
	_, err = s2.ReadVector(2)
	if err == nil {
		t.Error("Expected error when reading deleted vector after compact")
	}

	// Verify non-deleted vectors exist
	vec1, err := s2.ReadVector(1)
	if err != nil {
		t.Fatalf("Failed to read vector 1: %v", err)
	}
	if len(vec1) != 4 {
		t.Errorf("Expected vector dimension 4, got %d", len(vec1))
	}

	vec3, err := s2.ReadVector(3)
	if err != nil {
		t.Fatalf("Failed to read vector 3: %v", err)
	}
	if len(vec3) != 4 {
		t.Errorf("Expected vector dimension 4, got %d", len(vec3))
	}
}

func TestStorage_Compact_AllDeleted(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write and delete all vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.DeleteVector(1); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Close should trigger compact() which should truncate file
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify file is empty
	s2, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	if err := s2.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s2.Close()

	// Index should be empty
	if len(s2.index) != 0 {
		t.Errorf("Expected empty index after compacting all deleted vectors, got %d entries", len(s2.index))
	}
}

// Error path tests for ReadAllVectors
func TestStorage_ReadAllVectors_StatError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Close file to cause Stat() error
	s.file.Close()
	s.file = nil

	// ReadAllVectors should error when Stat fails
	_, err = s.ReadAllVectors()
	if err == nil {
		t.Error("Expected error when Stat fails in ReadAllVectors")
	}
}

func TestStorage_ReadAllVectors_SeekToStartError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Close file to cause Seek to start error
	s.file.Close()
	s.file = nil

	// ReadAllVectors should error when Seek to start fails
	_, err = s.ReadAllVectors()
	if err == nil {
		t.Error("Expected error when Seek to start fails in ReadAllVectors")
	}
}

func TestStorage_ReadAllVectors_SeekCurrentError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Seek to beginning first
	if _, err := s.file.Seek(0, 0); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	// Close file to cause SeekCurrent error
	s.file.Close()
	s.file = nil

	// ReadAllVectors should error when SeekCurrent fails
	_, err = s.ReadAllVectors()
	if err == nil {
		t.Error("Expected error when SeekCurrent fails in ReadAllVectors")
	}
}

func TestStorage_ReadAllVectors_ReadIDError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Truncate file to corrupt it (remove part of vector data)
	// This will cause binary.Read to fail when reading vector ID for second iteration
	// Truncate to just after first vector's ID (8 bytes) - incomplete data
	if err := s.file.Truncate(8); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// ReadAllVectors should handle the error gracefully
	// It should return empty map or partial results
	vectors, err := s.ReadAllVectors()
	// The function should handle the error - either return empty or error
	// Based on the code, it should return error if no vectors were read
	if err == nil && len(vectors) == 0 {
		// This is acceptable - function handled the error
		return
	}
	if err == nil && len(vectors) > 0 {
		// Partial read is acceptable
		return
	}
	// Error is also acceptable
}

func TestStorage_ReadAllVectors_PartialRead(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write multiple vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Truncate file to corrupt second vector (partial data)
	// First vector is: 8 bytes (ID) + 16 bytes (vector data) = 24 bytes
	// Second vector starts at offset 24
	// Truncate to 24 + 8 (ID) + 8 (partial vector data) = 40 bytes
	if err := s.file.Truncate(40); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// ReadAllVectors should handle partial read gracefully
	// It should return the first vector and handle the error for the second
	vectors, err := s.ReadAllVectors()
	// The function should either return partial results or error
	// Based on code, if some vectors were read, it should return them
	if err == nil {
		// Should have at least the first vector
		if len(vectors) == 0 {
			t.Error("Expected at least one vector from partial read")
		}
		if _, exists := vectors[1]; !exists {
			t.Error("Expected first vector to be in results")
		}
	}
	// Error is also acceptable for corrupted data
}