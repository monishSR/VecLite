package storage

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestNewStorage(t *testing.T) {
	s, err := NewStorage("test.db", 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
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
		2: {4.0, 5.0, 6.0, 7.0},
		3: {7.0, 8.0, 9.0, 10.0},
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

	s, err := NewStorage(tmpFile, 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
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

func TestClear(t *testing.T) {
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

	// Write some vectors
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(2, []float32{3.0, 4.0, 5.0, 6.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	if err := s.WriteVector(3, []float32{5.0, 6.0, 7.0, 8.0}); err != nil {
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

func TestSync(t *testing.T) {
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

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
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

	s, err := NewStorage(tmpFile, 4, 0)
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

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Try to write without opening
	err = s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0})
	if err == nil {
		t.Error("Expected error when writing without opening file")
	}
}

func TestReadVector_WithoutOpen(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Try to read without opening
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading without opening file")
	}
}

func TestNewStorage_ErrorCases(t *testing.T) {
	// Test invalid dimension
	_, err := NewStorage("test.db", 0, 0)
	if err == nil {
		t.Error("Expected error for dimension 0")
	}

	// Test negative dimension
	_, err = NewStorage("test.db", -1, 0)
	if err == nil {
		t.Error("Expected error for negative dimension")
	}
}

func TestStorage_GetCachedVector(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Create storage with cache enabled
	s, err := NewStorage(tmpFile, 4, 10) // Cache capacity 10
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// First read should populate cache
	vec1, err := s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	// Second read should hit cache (tested indirectly through ReadVector)
	vec2, err := s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	// Verify vectors match
	if len(vec1) != len(vec2) {
		t.Errorf("Vector length mismatch")
	}
	for i := range vec1 {
		if vec1[i] != vec2[i] {
			t.Errorf("Vector mismatch at index %d", i)
		}
	}
}

func TestStorage_GetCachedVector_Disabled(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Create storage with cache disabled
	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write and read should still work without cache
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	vec, err := s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}
	if len(vec) != 4 {
		t.Errorf("Expected vector dimension 4, got %d", len(vec))
	}
}

func TestStorage_Sync_WithoutOpen(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	// Sync without opening file should return nil (file is nil check)
	err = s.Sync()
	if err != nil {
		t.Errorf("Sync without open should return nil, got: %v", err)
	}
}

func TestStorage_Sync_WithOpen(t *testing.T) {
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

	// Write a vector
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Sync should succeed
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestStorage_GetFilePath(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	path := s.GetFilePath()
	if path != tmpFile {
		t.Errorf("Expected file path %s, got %s", tmpFile, path)
	}
}

func TestStorage_NewStorage_WithCache(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Test with cache enabled
	s, err := NewStorage(tmpFile, 4, 100)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write and read to test cache
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Read twice - second should hit cache
	vec1, err := s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	vec2, err := s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	// Verify vectors match
	if len(vec1) != len(vec2) {
		t.Errorf("Vector length mismatch")
	}
}

func TestStorage_NewStorage_NegativeCacheCapacity(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Test with negative cache capacity (should default to 1000)
	s, err := NewStorage(tmpFile, 4, -1)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Should work normally
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
}

func TestStorage_DeleteVector(t *testing.T) {
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

	// Delete a vector
	if err := s.DeleteVector(1); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}

	// Verify deleted vector is not in index
	if _, exists := s.index[1]; exists {
		t.Error("Deleted vector should not be in index")
	}

	// Verify other vector still exists
	if _, exists := s.index[2]; !exists {
		t.Error("Non-deleted vector should be in index")
	}

	// Try to read deleted vector (should fail)
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading deleted vector")
	}

	// Delete non-existent vector (should not error)
	if err := s.DeleteVector(999); err != nil {
		t.Errorf("DeleteVector of non-existent vector should not error, got: %v", err)
	}
}

func TestStorage_DeleteVector_WithCache(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 10)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write and read vector to populate cache
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	_, err = s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	// Delete should remove from cache
	if err := s.DeleteVector(1); err != nil {
		t.Fatalf("DeleteVector failed: %v", err)
	}
}

func TestStorage_Clear(t *testing.T) {
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

	// Clear all vectors
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify index is empty
	if len(s.index) != 0 {
		t.Errorf("Expected empty index after Clear, got %d entries", len(s.index))
	}

	// Verify vectors can't be read
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading vector after Clear")
	}
}

func TestStorage_Clear_WithCache(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 10)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Write and read vectors to populate cache
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}
	_, err = s.ReadVector(1)
	if err != nil {
		t.Fatalf("ReadVector failed: %v", err)
	}

	// Clear should purge cache
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
}

func TestStorage_ReadVector_IDMismatch(t *testing.T) {
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

	// Write a vector with ID 1
	if err := s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0}); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Corrupt the file by writing wrong ID at the vector's offset
	offset, exists := s.index[1]
	if !exists {
		t.Fatal("Vector should be in index")
	}

	if _, err := s.file.Seek(offset, 0); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	// Write wrong ID
	if err := binary.Write(s.file, binary.LittleEndian, uint64(999)); err != nil {
		t.Fatalf("Failed to write wrong ID: %v", err)
	}

	// ReadVector should detect ID mismatch
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error for ID mismatch")
	}
}

func TestStorage_ReadVector_SeekError(t *testing.T) {
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

	// Close file to cause seek error
	s.file.Close()
	s.file = nil

	// ReadVector should error
	_, err = s.ReadVector(1)
	if err == nil {
		t.Error("Expected error when reading with closed file")
	}
}

func TestStorage_WriteVector_SeekError(t *testing.T) {
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	s, err := NewStorage(tmpFile, 4, 0)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close file to cause seek error
	s.file.Close()
	s.file = nil

	// WriteVector should error
	err = s.WriteVector(1, []float32{1.0, 2.0, 3.0, 4.0})
	if err == nil {
		t.Error("Expected error when writing with closed file")
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