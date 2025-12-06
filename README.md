# VecLite

An embedded Vector DB written entirely in golang.

## Project Structure

```
VecLite/
├── cmd/
│   └── example/          # Example usage of VecLite
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
│   │   └── ivf/          # IVF (Inverted File) index (planned)
│   │       └── ivf.go
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

- **Multiple Index Types**: Support for Flat and HNSW indexes (IVF planned)
- **Vector Operations**: L2 distance, cosine distance, dot product, normalization
- **Persistent Storage**: On-disk storage with efficient ID-to-offset indexing and LRU cache
- **Thread-Safe**: Concurrent read/write operations with RWMutex
- **Memory Efficient**: Vectors stored on disk, only graph structure in memory (HNSW)
- **Embedded**: Single binary, minimal external dependencies

## Quick Start

```go
package main

import (
    "github.com/msr23/veclite/pkg/veclite"
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

## Index Types

### Flat Index

The Flat index is a brute-force search implementation that provides exact nearest neighbor search. It maintains a set of vector IDs in memory and performs a linear scan through all vectors during search, computing distances for each vector. Vectors are stored on disk and accessed through the storage layer's LRU cache for efficient retrieval. This index type is ideal for small to medium-sized datasets (up to ~10K vectors) where exact results are required and search speed is acceptable. The Flat index offers O(n) search complexity where n is the number of vectors, making it simple and reliable but slower for large datasets.

### HNSW Index

The HNSW (Hierarchical Navigable Small World) index is an approximate nearest neighbor search algorithm that provides sub-linear search complexity. It builds a multi-layer graph structure where each layer is a small-world network, enabling fast navigation from entry points to nearest neighbors. The implementation is memory-efficient, storing only the graph structure (node IDs and connections) in memory while keeping vectors on disk. The graph structure is persisted to a separate `.graph` file for fast index loading. HNSW is optimized for large datasets (100K+ vectors) and provides configurable trade-offs between search quality and speed through parameters like `M` (connections per node), `efConstruction` (search width during insertion), and `efSearch` (search width during query). The index includes CPU optimizations such as early termination, selective neighbor exploration, and reduced iteration limits for better performance on large datasets.

## Development Status

- ✅ Project structure with organized folder layout
- ✅ Flat index implementation (fully functional)
- ✅ HNSW index implementation (fully functional with graph persistence)
- ✅ Vector operations (L2, cosine, dot product, normalization)
- ✅ Persistent storage layer with ID-to-offset indexing and LRU cache
- ✅ Thread-safe concurrent operations (RWMutex)
- ✅ Comprehensive test coverage for all index types
- ✅ Parallel read/write tests
- ✅ Performance benchmarks
- 🚧 IVF index (planned)
- 🚧 Async index updates (planned)

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

- **Insert Benchmarks**: `BenchmarkInsert_Flat`, `BenchmarkInsert_HNSW`
- **Search Benchmarks**: 
  - Small dataset (10K): `BenchmarkSearch_Flat`, `BenchmarkSearch_HNSW`
  - Large dataset (100K): `BenchmarkSearch_Flat_LargeDataset`, `BenchmarkSearch_HNSW_LargeDataset`
  - Very large dataset (1M): `BenchmarkSearch_Flat_VeryLargeDataset`, `BenchmarkSearch_HNSW_VeryLargeDataset`
- **Read Benchmarks**: `BenchmarkRead_Flat`, `BenchmarkRead_HNSW`

### Benchmark Configuration

The benchmarks use the following HNSW parameters for optimal performance:
- `M = 16`: Maximum connections per node
- `EfConstruction = 64`: Search width during construction (reduced from default 200 for faster insertion)
- `EfSearch = 10`: Search width during query (optimized for smaller datasets)

Note: For production use with large datasets, you may want to increase `EfConstruction` to 200 and `EfSearch` to 50-100 for better search quality.

### Benchmark Results

The following table shows performance benchmarks for different operations on a dataset of 10,000 vectors (128 dimensions):

| Operation | Index Type | Time per Operation | Notes |
|-----------|------------|-------------------|-------|
| **Insert** | Flat | ~0.035 ms (35,349 ns/op) | Fast, direct write to storage |
| **Insert** | HNSW | ~0.275 ms (274,694 ns/op) | Slower due to graph construction, ~7.8x slower than Flat |
| **Search** | Flat | ~85.4 ms (85,441,444 ns/op) | Linear scan through all vectors |
| **Search** | HNSW | *Optimizing* | Currently being optimized for better performance |
| **Read** | Flat | ~2.0 ms (1,989,865 ns/op) | Direct storage read with cache |
| **Read** | HNSW | *Optimizing* | Similar to Flat (uses same storage layer) |

**Performance Characteristics:**
- **Flat Index**: Provides exact search results but requires scanning all vectors, making it O(n) complexity. Best for small datasets (<10K vectors) where exact results are required.
- **HNSW Index**: Insertion is slower due to graph construction overhead, but search should be significantly faster for large datasets once optimized. The graph structure enables sub-linear search complexity.

**Test Environment:**
- Dataset size: 10,000 vectors
- Vector dimension: 128
- Cache capacity: 1,000 vectors
- HNSW parameters: M=16, EfConstruction=64, EfSearch=10

*Note: HNSW search benchmarks are currently being optimized. Results will be updated as performance improvements are made.*

## License

MIT License - see LICENSE file for details.
