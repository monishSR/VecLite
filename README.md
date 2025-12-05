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
│   │   ├── index.go      # Index interface
│   │   ├── flat.go       # Flat (brute-force) index
|   |   ├── flat_test.go
│   │   ├── hnsw.go       # HNSW index (placeholder)
│   │   └── ivf.go        # IVF index (placeholder)
│   ├── storage/          # Persistent storage layer
│   │   └── storage.go
│   └── vector/           # Vector operations (distance, normalization, etc.)
│       ├── vector.go
│       └── vector_test.go
├── pkg/
│   └── veclite/          # Public API for VecLite
│       ├── veclite.go
│       └── veclite_test.go
├── go.mod                # Go module definition
├── Makefile              # Build and test commands
├── .gitignore
├── LICENSE
├── CONTRIBUTING.md
└── README.md
```

## Features

- **Multiple Index Types**: Support for Flat index (HNSW and IVF planned)
- **Vector Operations**: L2 distance, cosine distance, dot product, normalization
- **Persistent Storage**: On-disk storage with efficient ID-to-offset indexing
- **Thread-Safe**: Concurrent read/write operations with RWMutex
- **Embedded**: Single binary, no external dependencies

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

## Development Status

- ✅ Project structure
- ✅ Flat index implementation (fully functional)
- ✅ Vector operations (L2, cosine, dot product, normalization)
- ✅ Persistent storage layer with index persistence
- ✅ Thread-safe concurrent operations (RWMutex)
- ✅ Comprehensive test coverage (~60%+)
- ✅ Parallel read/write tests
- 🚧 HNSW index (planned)
- 🚧 IVF index (planned)

## Features in Detail

- **Flat Index**: Brute-force search with full vector storage in memory
- **Storage**: Persistent on-disk storage with ID-to-offset indexing
- **Concurrency**: Thread-safe operations using read-write locks
- **Vector Operations**: Efficient L2 distance, cosine similarity, dot product
- **Search Results**: Returns ID, distance, and full vector data

## License

MIT License - see LICENSE file for details.
