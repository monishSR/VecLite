package veclite

import (
	"math/rand"
	"os"
	"testing"
)

// Benchmarking Guide:
//
// Run all benchmarks:
//   go test ./pkg/veclite -bench=. -run='^$'
//
// Run specific benchmark:
//   go test ./pkg/veclite -bench=BenchmarkSearch_Flat -run='^$'
//
// Compare Flat vs HNSW:
//   go test ./pkg/veclite -bench=BenchmarkSearch -run='^$' | grep Benchmark
//
// Run with more iterations for better accuracy:
//   go test ./pkg/veclite -bench=. -benchtime=5s -run='^$'

// createBenchmarkDB creates a database for benchmarking
func createBenchmarkDB(b *testing.B, indexType string) (*VecLite, func()) {
	tmpFile, err := os.CreateTemp("", "veclite_bench_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	config := DefaultConfig()
	config.DataPath = tmpFile.Name()
	config.Dimension = 128
	config.IndexType = indexType
	config.CacheCapacity = 1000 // Enable cache for fair comparison

	// Set HNSW parameters if needed
	if indexType == "hnsw" {
		config.M = 16
		config.EfConstruction = 64 // Reduced from 200 for faster insertion
		config.EfSearch = 10       // Reduced further for faster search on smaller datasets
	}

	db, err := New(config)
	if err != nil {
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph")
		b.Fatalf("Failed to create database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph")
	}

	return db, cleanup
}

// generateRandomVector generates a random vector for testing
func generateRandomVector(dimension int, seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed))
	vector := make([]float32, dimension)
	for i := range vector {
		vector[i] = rng.Float32()
	}
	return vector
}

// BenchmarkInsert_Flat benchmarks insert performance for flat index
func BenchmarkInsert_Flat(b *testing.B) {
	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	vectors := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		vectors[i] = generateRandomVector(128, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(uint64(i+1), vectors[i]); err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}
}

// BenchmarkInsert_HNSW benchmarks insert performance for HNSW index
func BenchmarkInsert_HNSW(b *testing.B) {
	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	vectors := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		vectors[i] = generateRandomVector(128, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(uint64(i+1), vectors[i]); err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}
}

// BenchmarkSearch_Flat benchmarks search performance for flat index
func BenchmarkSearch_Flat(b *testing.B) {
	const datasetSize = 10000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_HNSW benchmarks search performance for HNSW index
func BenchmarkSearch_HNSW(b *testing.B) {
	const datasetSize = 10000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_Flat_LargeDataset benchmarks search on larger dataset
func BenchmarkSearch_Flat_LargeDataset(b *testing.B) {
	const datasetSize = 100000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_HNSW_LargeDataset benchmarks search on larger dataset
func BenchmarkSearch_HNSW_LargeDataset(b *testing.B) {
	const datasetSize = 100000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_Flat_VeryLargeDataset benchmarks search on very large dataset
func BenchmarkSearch_Flat_VeryLargeDataset(b *testing.B) {
	const datasetSize = 1000000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_HNSW_VeryLargeDataset benchmarks search on very large dataset
func BenchmarkSearch_HNSW_VeryLargeDataset(b *testing.B) {
	const datasetSize = 1000000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		queries[i] = generateRandomVector(128, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkRead_Flat benchmarks read performance for flat index
func BenchmarkRead_Flat(b *testing.B) {
	const datasetSize = 10000

	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate random IDs to read
	ids := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = uint64((i % datasetSize) + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(ids[i])
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

// BenchmarkRead_HNSW benchmarks read performance for HNSW index
func BenchmarkRead_HNSW(b *testing.B) {
	const datasetSize = 10000

	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	// Insert dataset
	for i := 0; i < datasetSize; i++ {
		vector := generateRandomVector(128, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate random IDs to read
	ids := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = uint64((i % datasetSize) + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(ids[i])
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}
