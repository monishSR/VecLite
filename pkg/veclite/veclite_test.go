package veclite

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// createTestDB creates a temporary database for testing with specified index type
func createTestDB(t *testing.T, indexType string) (*VecLite, func()) {
	tmpFile, err := os.CreateTemp("", "veclite_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	config := DefaultConfig()
	config.DataPath = tmpFile.Name()
	config.Dimension = 128
	config.IndexType = indexType

	// Set HNSW parameters if needed
	if indexType == "hnsw" {
		config.M = 16
		config.EfConstruction = 200
		config.EfSearch = 50
	}

	db, err := New(config)
	if err != nil {
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph") // Clean up graph file if it exists
		t.Fatalf("Failed to create database with index type %s: %v", indexType, err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph") // Clean up graph file for HNSW
	}

	return db, cleanup
}

// runTestForAllIndexes runs a test function for all supported index types
func runTestForAllIndexes(t *testing.T, testFunc func(t *testing.T, indexType string)) {
	indexTypes := []string{"flat", "hnsw"}
	// TODO: Add "ivf" when implemented

	for _, indexType := range indexTypes {
		t.Run(indexType, func(t *testing.T) {
			testFunc(t, indexType)
		})
	}
}

func TestVecLite_ParallelWrites(t *testing.T) {
	runTestForAllIndexes(t, func(t *testing.T, indexType string) {
		db, cleanup := createTestDB(t, indexType)
		defer cleanup()

		const numGoroutines = 10
		const vectorsPerGoroutine = 10
		const dimension = 128

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*vectorsPerGoroutine)

		// Launch multiple goroutines to insert vectors concurrently
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < vectorsPerGoroutine; j++ {
					id := uint64(goroutineID*vectorsPerGoroutine + j + 1)
					vector := make([]float32, dimension)
					// Fill vector with unique values based on ID
					for k := range vector {
						vector[k] = float32(id) + float32(k)*0.001
					}

					if err := db.Insert(id, vector); err != nil {
						errors <- err
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Insert error: %v", err)
		}

		// Verify all vectors were inserted
		expectedSize := numGoroutines * vectorsPerGoroutine
		actualSize := db.Size()
		if actualSize != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, actualSize)
		}

		// Verify we can read all vectors
		for i := uint64(1); i <= uint64(expectedSize); i++ {
			vector, err := db.Get(i)
			if err != nil {
				t.Errorf("Failed to read vector %d: %v", i, err)
				continue
			}
			if len(vector) != dimension {
				t.Errorf("Vector %d has wrong dimension: expected %d, got %d", i, dimension, len(vector))
			}
			// Verify vector values
			expectedFirstValue := float32(i)
			if vector[0] != expectedFirstValue {
				t.Errorf("Vector %d[0] mismatch: expected %f, got %f", i, expectedFirstValue, vector[0])
			}
		}
	})
}

func TestVecLite_ParallelSearches(t *testing.T) {
	runTestForAllIndexes(t, func(t *testing.T, indexType string) {
		db, cleanup := createTestDB(t, indexType)
		defer cleanup()

		const numVectors = 100
		const dimension = 128

		// Insert vectors first
		for i := uint64(1); i <= numVectors; i++ {
			vector := make([]float32, dimension)
			for j := range vector {
				vector[j] = float32(i) + float32(j)*0.001
			}
			if err := db.Insert(i, vector); err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		const numGoroutines = 15
		const searchesPerGoroutine = 5

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*searchesPerGoroutine)

		// Launch multiple goroutines to search concurrently
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < searchesPerGoroutine; j++ {
					// Create query vector
					query := make([]float32, dimension)
					for k := range query {
						query[k] = float32(goroutineID*searchesPerGoroutine+j) + float32(k)*0.001
					}

					results, err := db.Search(query, 5)
					if err != nil {
						errors <- err
						continue
					}
					if len(results) == 0 {
						errors <- fmt.Errorf("search returned no results")
						continue
					}
					// Verify results have vectors
					for _, result := range results {
						if result.Vector == nil || len(result.Vector) != dimension {
							errors <- fmt.Errorf("search result missing vector or wrong dimension")
							break
						}
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Search error: %v", err)
		}
	})
}

func TestVecLite_ParallelWritesAndReads(t *testing.T) {
	runTestForAllIndexes(t, func(t *testing.T, indexType string) {
		db, cleanup := createTestDB(t, indexType)
		defer cleanup()

		const numWriters = 5
		const numReaders = 10
		const vectorsPerWriter = 20
		const dimension = 128
		const readsPerReader = 30

		var wg sync.WaitGroup
		writeErrors := make(chan error, numWriters*vectorsPerWriter)
		readErrors := make(chan error, numReaders*readsPerReader)

		// Writers: Insert vectors concurrently
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < vectorsPerWriter; j++ {
					id := uint64(writerID*vectorsPerWriter + j + 1)
					vector := make([]float32, dimension)
					for k := range vector {
						vector[k] = float32(id) + float32(k)*0.001
					}

					if err := db.Insert(id, vector); err != nil {
						writeErrors <- err
					}
				}
			}(i)
		}

		// Readers: Read vectors concurrently (may read while writes are happening)
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()
				for j := 0; j < readsPerReader; j++ {
					// Try to read various IDs (some may not exist yet)
					id := uint64((readerID*readsPerReader + j) % (numWriters * vectorsPerWriter))
					if id == 0 {
						id = 1
					}
					vector, err := db.Get(id)
					// It's OK if vector doesn't exist yet (read during write)
					if err == nil && len(vector) != dimension {
						readErrors <- fmt.Errorf("vector %d has wrong dimension: expected %d, got %d", id, dimension, len(vector))
					}
				}
			}(i)
		}

		wg.Wait()
		close(writeErrors)
		close(readErrors)

		// Check for write errors
		for err := range writeErrors {
			t.Errorf("Write error: %v", err)
		}

		// Check for read errors (only report dimension mismatches, not "not found")
		for err := range readErrors {
			t.Errorf("Read error: %v", err)
		}

		// Verify final state
		expectedSize := numWriters * vectorsPerWriter
		actualSize := db.Size()
		if actualSize != expectedSize {
			t.Errorf("Expected final size %d, got %d", expectedSize, actualSize)
		}
	})
}

func TestVecLite_ParallelWritesAndSearches(t *testing.T) {
	runTestForAllIndexes(t, func(t *testing.T, indexType string) {
		db, cleanup := createTestDB(t, indexType)
		defer cleanup()

		const numWriters = 5
		const numSearchers = 8
		const vectorsPerWriter = 15
		const searchesPerSearcher = 10
		const dimension = 128

		var wg sync.WaitGroup
		writeErrors := make(chan error, numWriters*vectorsPerWriter)
		searchErrors := make(chan error, numSearchers*searchesPerSearcher)

		// Writers: Insert vectors concurrently
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < vectorsPerWriter; j++ {
					id := uint64(writerID*vectorsPerWriter + j + 1)
					vector := make([]float32, dimension)
					for k := range vector {
						vector[k] = float32(id) + float32(k)*0.001
					}

					if err := db.Insert(id, vector); err != nil {
						writeErrors <- err
					}
				}
			}(i)
		}

		// Searchers: Search concurrently (may search while writes are happening)
		for i := 0; i < numSearchers; i++ {
			wg.Add(1)
			go func(searcherID int) {
				defer wg.Done()
				for j := 0; j < searchesPerSearcher; j++ {
					// Create query vector
					query := make([]float32, dimension)
					for k := range query {
						query[k] = float32(searcherID*searchesPerSearcher+j) + float32(k)*0.001
					}

					results, err := db.Search(query, 3)
					if err != nil {
						searchErrors <- err
						continue
					}
					// Verify results have vectors if any results returned
					for _, result := range results {
						if result.Vector != nil && len(result.Vector) != dimension {
							searchErrors <- fmt.Errorf("search result has wrong vector dimension: expected %d, got %d", dimension, len(result.Vector))
						}
					}
				}
			}(i)
		}

		wg.Wait()
		close(writeErrors)
		close(searchErrors)

		// Check for write errors
		for err := range writeErrors {
			t.Errorf("Write error: %v", err)
		}

		// Check for search errors
		for err := range searchErrors {
			t.Errorf("Search error: %v", err)
		}

		// Verify final state
		expectedSize := numWriters * vectorsPerWriter
		actualSize := db.Size()
		if actualSize != expectedSize {
			t.Errorf("Expected final size %d, got %d", expectedSize, actualSize)
		}
	})
}

func TestVecLite_ParallelMixedOperations(t *testing.T) {
	runTestForAllIndexes(t, func(t *testing.T, indexType string) {
		db, cleanup := createTestDB(t, indexType)
		defer cleanup()

		const numOperations = 20
		const dimension = 128

		// Insert some initial vectors
		for i := uint64(1); i <= 10; i++ {
			vector := make([]float32, dimension)
			for j := range vector {
				vector[j] = float32(i) + float32(j)*0.001
			}
			if err := db.Insert(i, vector); err != nil {
				t.Fatalf("Failed to insert initial vector %d: %v", i, err)
			}
		}

		var wg sync.WaitGroup
		errors := make(chan error, numOperations*3)

		// Mix of operations: writes, reads, and searches
		for i := 0; i < numOperations; i++ {
			// Writer
			wg.Add(1)
			go func(opID int) {
				defer wg.Done()
				id := uint64(10 + opID + 1)
				vector := make([]float32, dimension)
				for j := range vector {
					vector[j] = float32(id) + float32(j)*0.001
				}
				if err := db.Insert(id, vector); err != nil {
					errors <- err
				}
			}(i)

			// Reader
			wg.Add(1)
			go func(opID int) {
				defer wg.Done()
				id := uint64((opID % 10) + 1)
				vector, err := db.Get(id)
				if err == nil && len(vector) != dimension {
					errors <- fmt.Errorf("read vector %d has wrong dimension", id)
				}
			}(i)

			// Searcher
			wg.Add(1)
			go func(opID int) {
				defer wg.Done()
				query := make([]float32, dimension)
				for j := range query {
					query[j] = float32(opID) + float32(j)*0.001
				}
				results, err := db.Search(query, 3)
				if err != nil {
					errors <- err
				} else {
					for _, result := range results {
						if result.Vector != nil && len(result.Vector) != dimension {
							errors <- fmt.Errorf("search result has wrong vector dimension")
						}
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		errorCount := 0
		for err := range errors {
			t.Errorf("Operation error: %v", err)
			errorCount++
		}

		if errorCount > 0 {
			t.Errorf("Encountered %d errors during parallel operations", errorCount)
		}

		// Verify final state
		expectedSize := 10 + numOperations
		actualSize := db.Size()
		if actualSize != expectedSize {
			t.Errorf("Expected final size %d, got %d", expectedSize, actualSize)
		}
	})
}
