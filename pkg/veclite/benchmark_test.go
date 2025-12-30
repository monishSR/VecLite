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
// Run with clustered data (more realistic, better for HNSW):
//   go test ./pkg/veclite -bench=BenchmarkSearch_HNSW_Clustered -run='^$'
//
// Run with more iterations for better accuracy:
//   go test ./pkg/veclite -bench=. -benchtime=5s -run='^$'
//
// Using Real Embeddings:
//   See BENCHMARKING.md for detailed instructions on using real embeddings
//   from ML models (BERT, sentence-transformers, etc.)

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
	// Set IVF parameters if needed
	if indexType == "ivf" {
		config.NClusters = 100 // Default: √N for 10K vectors
		config.NProbe = 10     // Search 10 clusters for better recall
	}

	db, err := New(config)
	if err != nil {
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph")
		os.Remove(tmpFile.Name() + ".ivf")
		b.Fatalf("Failed to create database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".graph")
		os.Remove(tmpFile.Name() + ".ivf")
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

// generateClusteredVector generates a vector from a Gaussian cluster
// This creates structured data that better represents real-world embeddings
// clusterID determines which cluster the vector belongs to
// numClusters is the total number of clusters
// dimension is the vector dimension
// seed ensures reproducibility
func generateClusteredVector(dimension, clusterID, numClusters int, seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed))
	vector := make([]float32, dimension)

	// Create cluster centroids spread across the space
	// Each cluster has a centroid at a different location
	centroidOffset := float32(clusterID) / float32(numClusters)

	// Generate vector with Gaussian noise around the centroid
	for i := range vector {
		// Base value based on cluster position
		base := centroidOffset + float32(i%10)*0.1
		// Add Gaussian noise (mean=0, std=0.1)
		noise := float32(rng.NormFloat64()) * 0.1
		vector[i] = base + noise
		// Normalize to [0, 1] range
		if vector[i] < 0 {
			vector[i] = 0
		} else if vector[i] > 1 {
			vector[i] = 1
		}
	}

	return vector
}

// loadVectorsFromFile loads vectors from a binary file (optional helper)
// Format: [numVectors uint32][dimension uint32][vector1...][vector2...]
// Each vector is dimension * float32 values
// This is useful for loading pre-computed embeddings
func loadVectorsFromFile(filename string) ([][]float32, error) {
	// This is a placeholder - implement based on your file format
	// Example formats: binary, CSV, JSON, etc.
	return nil, nil
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

// BenchmarkInsert_IVF benchmarks insert performance for IVF index
func BenchmarkInsert_IVF(b *testing.B) {
	db, cleanup := createBenchmarkDB(b, "ivf")
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

// BenchmarkSearch_Flat_Clustered benchmarks Flat search with clustered data
// This allows fair comparison with HNSW on structured data
func BenchmarkSearch_Flat_Clustered(b *testing.B) {
	const datasetSize = 10000
	const k = 10
	const numClusters = 50 // Number of clusters in the dataset

	db, cleanup := createBenchmarkDB(b, "flat")
	defer cleanup()

	// Insert clustered dataset
	for i := 0; i < datasetSize; i++ {
		clusterID := i % numClusters
		vector := generateClusteredVector(128, clusterID, numClusters, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors from random clusters
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		// Query from a random cluster
		queryClusterID := (i + datasetSize) % numClusters
		queries[i] = generateClusteredVector(128, queryClusterID, numClusters, int64(i+datasetSize))
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

// BenchmarkSearch_HNSW_Clustered benchmarks HNSW search with clustered data
// This better represents real-world performance where vectors have structure
func BenchmarkSearch_HNSW_Clustered(b *testing.B) {
	const datasetSize = 10000
	const k = 10
	const numClusters = 50 // Number of clusters in the dataset

	db, cleanup := createBenchmarkDB(b, "hnsw")
	defer cleanup()

	// Insert clustered dataset
	for i := 0; i < datasetSize; i++ {
		clusterID := i % numClusters
		vector := generateClusteredVector(128, clusterID, numClusters, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors from random clusters
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		// Query from a random cluster
		queryClusterID := (i + datasetSize) % numClusters
		queries[i] = generateClusteredVector(128, queryClusterID, numClusters, int64(i+datasetSize))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Search(queries[i], k)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}

// BenchmarkSearch_IVF benchmarks search performance for IVF index
func BenchmarkSearch_IVF(b *testing.B) {
	const datasetSize = 10000
	const k = 10

	db, cleanup := createBenchmarkDB(b, "ivf")
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

// BenchmarkSearch_IVF_Clustered benchmarks IVF search with clustered data
// IVF works best with clustered data since it uses cluster centroids
func BenchmarkSearch_IVF_Clustered(b *testing.B) {
	const datasetSize = 10000
	const k = 10
	const numClusters = 50 // Number of clusters in the dataset

	db, cleanup := createBenchmarkDB(b, "ivf")
	defer cleanup()

	// Insert clustered dataset
	for i := 0; i < datasetSize; i++ {
		clusterID := i % numClusters
		vector := generateClusteredVector(128, clusterID, numClusters, int64(i))
		if err := db.Insert(uint64(i+1), vector); err != nil {
			b.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Generate query vectors from random clusters
	queries := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		// Query from a random cluster
		queryClusterID := (i + datasetSize) % numClusters
		queries[i] = generateClusteredVector(128, queryClusterID, numClusters, int64(i+datasetSize))
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

// BenchmarkSearch_IVF_LargeDataset benchmarks IVF search on larger dataset
func BenchmarkSearch_IVF_LargeDataset(b *testing.B) {
	const datasetSize = 100000
	const k = 10

	// Use more clusters for larger dataset
	tmpFile, err := os.CreateTemp("", "veclite_bench_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	config := DefaultConfig()
	config.DataPath = tmpFile.Name()
	config.Dimension = 128
	config.IndexType = "ivf"
	config.CacheCapacity = 1000
	config.NClusters = 316 // √N for 100K vectors
	config.NProbe = 10

	db, err := New(config)
	if err != nil {
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".ivf")
		b.Fatalf("Failed to create database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
		os.Remove(tmpFile.Name() + ".ivf")
	}
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

// BenchmarkRead_IVF benchmarks read performance for IVF index
func BenchmarkRead_IVF(b *testing.B) {
	const datasetSize = 10000

	db, cleanup := createBenchmarkDB(b, "ivf")
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
