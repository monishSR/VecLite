<div style="text-align: left;">
  <img src="assets/icon.svg" alt="VecLite Logo" width="900" height="214" style="display: block; margin: 0;">
</div>

[![Go Version](https://img.shields.io/badge/go-1.21+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/monishSR/veclite?style=flat-square)](https://goreportcard.com/report/github.com/monishSR/veclite)
[![Go Reference](https://pkg.go.dev/badge/github.com/monishSR/veclite.svg)](https://pkg.go.dev/github.com/monishSR/veclite)

**A lightweight, embedded vector database written in Go. Perfect for adding semantic search, similarity matching, and ANN capabilities directly into your Go applications.**

## Why VecLite?

Building AI-powered features like semantic search, recommendation systems, or similarity matching shouldn't require managing complex infrastructure. VecLite brings vector search capabilities directly into your Go application:

- **Zero Infrastructure**: No separate database server, no network calls - just import and use
- **Single Binary**: Minimal dependencies, perfect for microservices and embedded systems
- **Fast & Efficient**: HNSW algorithm for sub-linear search, LRU caching, and memory-efficient storage
- **Thread-Safe**: Built-in concurrency support for read-heavy workloads
- **Persistent**: Data survives restarts with efficient on-disk storage

**Perfect for**: Semantic search, recommendation engines, duplicate detection, clustering, and any application needing similarity search in Go.

## Quick Example

```go
package main

import (
    "fmt"
    "github.com/monishSR/veclite/pkg/veclite"
)

func main() {
    // Create database
    config := veclite.DefaultConfig()
    config.Dimension = 128
    config.IndexType = "hnsw"  // or "flat" for exact search, "ivf" for very large datasets
    config.DataPath = "./vectors.db"
    
    db, _ := veclite.New(config)
    defer db.Close()
    
    // Insert vectors
    db.Insert(1, []float32{0.1, 0.2, 0.3, /* ... */})
    db.Insert(2, []float32{0.4, 0.5, 0.6, /* ... */})
    
    // Search for similar vectors
    results, _ := db.Search([]float32{0.15, 0.25, 0.35, /* ... */}, 5)
    for _, r := range results {
        fmt.Printf("ID: %d, Distance: %.4f\n", r.ID, r.Distance)
    }
}
```

**See [examples/basic/main.go](examples/basic/main.go) for a complete example demonstrating Insert, Search, and Persistence.**

## Project Structure

```
VecLite/
├── assets/               # Project assets (logo, images, etc.)
│   └── icon.svg
├── examples/             # Example usage of VecLite
│   └── basic/            # Basic example (Insert, Search, Persistence)
│       └── main.go
├── internal/             # Private application code
│   ├── index/            # Indexing structures (HNSW, IVF, Flat)
│   │   ├── index.go      # Index interface and factory
│   │   ├── types/         # Shared types and errors
│   │   │   └── types.go
│   │   ├── utils/        # Shared utilities (heap, etc.)
│   │   │   ├── heap.go
│   │   │   └── heap_test.go
│   │   ├── flat/         # Flat (brute-force) index
│   │   │   ├── flat.go
│   │   │   └── flat_test.go
│   │   ├── hnsw/         # HNSW (Hierarchical Navigable Small World) index
│   │   │   ├── hnsw.go    # Core HNSW implementation
│   │   │   ├── graph.go   # Graph persistence operations
│   │   │   └── hnsw_test.go
│   │   └── ivf/          # IVF (Inverted File) index
│   │       ├── ivf.go    # Core IVF implementation
│   │       ├── centroid.go # Centroid management
│   │       ├── ivf_persistence.go # IVF persistence operations
│   │       ├── ivf_test.go
│   │       ├── centroid_test.go
│   │       └── ivf_persistence_test.go
│   ├── storage/          # Persistent storage layer
│   │   ├── storage.go
│   │   └── storage_test.go
│   └── vector/           # Vector operations (distance, normalization, etc.)
│       ├── vector.go
│       └── vector_test.go
├── pkg/
│   └── veclite/          # Public API for VecLite
│       ├── veclite.go
│       ├── veclite_test.go
│       └── benchmark_test.go  # Performance benchmarks
├── go.mod                # Go module definition
├── Makefile              # Build and test commands
├── .gitignore
├── LICENSE
├── CONTRIBUTING.md
└── README.md
```

## Features

- **Multiple Index Types**: Support for Flat, HNSW, and IVF indexes
- **Vector Operations**: L2 distance, cosine distance, dot product, normalization
- **Persistent Storage**: On-disk storage with efficient ID-to-offset indexing and LRU cache
- **Thread-Safe**: Concurrent read operations with exclusive write locking
- **Memory Efficient**: Vectors stored on disk, only index structure in memory
- **Embedded**: Single binary, minimal external dependencies

## Concurrency Model

VecLite uses a **read-write lock (RWMutex)** for thread safety:

- **Multiple Concurrent Reads**: `Search()`, `Get()`, and `Size()` can run simultaneously across goroutines
- **Single Writer**: `Insert()`, `Delete()`, and `Close()` are exclusive - only one write operation at a time
- **No Concurrent Read+Write**: Write operations block all reads until completion

**Example**: Multiple `Search()` calls can run concurrently, but `Insert()` blocks all reads and other writes. Optimized for **read-heavy workloads** with occasional writes.

## Quick Start

```go
package main

import (
    "github.com/monishSR/veclite/pkg/veclite"
)

func main() {
    // Create a new VecLite instance
    config := veclite.DefaultConfig()
    config.Dimension = 128
    config.DataPath = "./veclite.db"
    
    db, err := veclite.New(config)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Insert a vector
    vector := make([]float32, 128)
    // ... populate vector ...
    db.Insert(1, vector)
    
    // Search for similar vectors
    results, err := db.Search(query, 10)
    // ... use results ...
}
```

## Building

```bash
# Build the project
make build

# Run tests
make test

# Run example
make example
```

## Index Comparison

| Feature | Flat Index | HNSW Index | IVF Index |
|---------|-----------|------------|-----------|
| **Search Type** | Exact (100% recall) | Approximate (high recall) | Approximate (high recall) |
| **Search Complexity** | O(n) - linear scan | O(log n) - sub-linear | O(n/k) - cluster-based |
| **Best For** | Small datasets (<10K) | Large datasets (100K+) | Very large datasets (1M+) |
| **Memory Usage** | Low (only IDs) | Low (graph structure only) | Low (cluster structure only) |
| **Insert Speed** | Fast (~0.021 ms) | Slower (~4.62 ms) | Medium (~0.094 ms) |
| **Search Speed (10K, clustered)** | ~62.9 ms | ~0.88 ms | ~7.79 ms |
| **Search Speed (100K+)** | Slow (linear) | Fast (sub-linear) | Fast (cluster-based) |
| **Use Case** | Exact results needed | Speed prioritized | Very large datasets |

**Recommendation**: 
- Use **Flat** for small datasets (<10K) requiring exact results
- Use **HNSW** for large datasets (100K+) where speed is critical
- Use **IVF** for very large datasets (1M+) with clustered data

## Index Types

### Flat Index

A brute-force search implementation providing **exact nearest neighbor search** with 100% recall. Performs a linear scan through all vectors, computing distances for each. Ideal for small to medium-sized datasets (up to ~10K vectors) where exact results are required. Offers O(n) search complexity - simple, reliable, but slower for large datasets.

### HNSW Index

A state-of-the-art approximate nearest neighbor search algorithm with **sub-linear search complexity**. Builds a multi-layer graph structure where each layer is a small-world network, enabling fast navigation from entry points to nearest neighbors. Memory-efficient (only graph structure in memory, vectors on disk), optimized for large datasets (100K+ vectors), and includes CPU optimizations for better performance. Configurable via `M`, `efConstruction`, and `efSearch` parameters.

### IVF Index

An **Inverted File** index optimized for very large datasets (1M+ vectors). Uses cluster-based search where vectors are organized into clusters with centroids. During search, only the `nProbe` nearest clusters are examined, significantly reducing the search space. Memory-efficient (only cluster structure and centroids in memory, vectors on disk), ideal for datasets with natural clustering. Configurable via `NClusters` (number of clusters, typically √N) and `NProbe` (number of clusters to search, typically 1-10). Best performance on structured/clustered data.

## Roadmap

### ✅ Completed (v0.1)
- Flat index with exact search
- HNSW index with approximate search
- Persistent storage with efficient indexing
- Thread-safe concurrent operations
- LRU caching for performance
- Comprehensive test coverage
- Performance benchmarks

### ✅ Completed (v0.2)
- IVF (Inverted File) index for very large datasets
- IVF persistence and loading
- Comprehensive IVF test coverage

### 🚧 In Progress (v0.3)
- Async index updates for non-blocking writes
- Query optimization improvements

### 🔮 Planned (v1.0)
- Cosine similarity optimization
- Batch insert operations
- Index statistics and monitoring
- Backup and restore functionality
- Multi-dimensional distance metrics

## Benchmarking

VecLite includes comprehensive benchmarks to compare performance across different index types. The benchmarks measure insert, search, and read performance for various dataset sizes.

### Running Benchmarks

```bash
# Run all benchmarks
go test ./pkg/veclite -bench=. -run='^$'

# Run specific benchmark (e.g., HNSW search)
go test ./pkg/veclite -bench=BenchmarkSearch_HNSW -run='^$'

# Compare Flat vs HNSW search
go test ./pkg/veclite -bench=BenchmarkSearch -run='^$' | grep Benchmark

# Run with more iterations for better accuracy
go test ./pkg/veclite -bench=. -benchtime=5s -run='^$'
```

### Available Benchmarks

- **Insert Benchmarks**: `BenchmarkInsert_Flat`, `BenchmarkInsert_HNSW`, `BenchmarkInsert_IVF`
- **Search Benchmarks**: 
  - Small dataset (10K): `BenchmarkSearch_Flat`, `BenchmarkSearch_HNSW`, `BenchmarkSearch_IVF`
  - Clustered data: `BenchmarkSearch_Flat_Clustered`, `BenchmarkSearch_HNSW_Clustered`, `BenchmarkSearch_IVF_Clustered`
  - Large dataset (100K): `BenchmarkSearch_Flat_LargeDataset`, `BenchmarkSearch_HNSW_LargeDataset`, `BenchmarkSearch_IVF_LargeDataset`
  - Very large dataset (1M): `BenchmarkSearch_Flat_VeryLargeDataset`, `BenchmarkSearch_HNSW_VeryLargeDataset`
- **Read Benchmarks**: `BenchmarkRead_Flat`, `BenchmarkRead_HNSW`, `BenchmarkRead_IVF`

### Benchmark Configuration

The benchmarks use the following parameters for optimal performance:

**HNSW:**
- `M = 16`: Maximum connections per node
- `EfConstruction = 64`: Search width during construction (reduced from default 200 for faster insertion)
- `EfSearch = 10`: Search width during query (optimized for smaller datasets)

**IVF:**
- `NClusters = 100`: Number of clusters (√N for 10K vectors, √N for larger datasets)
- `NProbe = 10`: Number of clusters to search (higher = better recall, slower search)

Note: For production use with large datasets, you may want to:
- HNSW: Increase `EfConstruction` to 200 and `EfSearch` to 50-100 for better search quality
- IVF: Adjust `NClusters` to √N and `NProbe` based on recall requirements (1-10 typical)

### Quick Benchmarks

Performance on 10,000 vectors (128 dimensions):

```
Operation          | Index | Time/Op      | Speedup
-------------------|-------|--------------|--------
Insert             | Flat  | ~0.021 ms    | Baseline
Insert             | HNSW  | ~4.62 ms     | 220x slower
Insert             | IVF   | ~0.094 ms    | 4.5x slower
Search (Clustered) | Flat  | ~62.9 ms     | Baseline
Search (Clustered) | HNSW  | ~0.88 ms     | 71x faster ⚡
Search (Clustered) | IVF   | ~7.79 ms     | 8.1x faster ⚡
Read               | Flat  | ~0.006 ms    | Baseline
Read               | IVF   | ~0.010 ms    | 1.6x slower (shared storage)
```

**Key Insights**: 
- **HNSW** excels on structured data - **71x faster search** than Flat on clustered data
- **IVF** provides good balance - **8.1x faster** than Flat, faster insert than HNSW
- **Insert**: Flat is fastest, IVF is 4.5x slower but much faster than HNSW
- **Search**: HNSW is fastest, IVF is competitive and much faster than Flat

**Test Environment**: 10K vectors, 128 dims, clustered data (50 Gaussian clusters)
- HNSW: M=16, EfConstruction=64, EfSearch=10
- IVF: NClusters=100, NProbe=10

*For production with real embeddings, HNSW advantage increases on larger datasets (100K+ vectors). IVF is ideal for very large datasets (1M+ vectors) with natural clustering.*

## Installation

```bash
go get github.com/monishSR/veclite
```

## Package Distribution

To use VecLite in your project:
```go
import "github.com/monishSR/veclite/pkg/veclite"
```

Then run `go mod tidy` to download dependencies.

## License

MIT License - see LICENSE file for details.
