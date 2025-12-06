package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/msr23/veclite/pkg/veclite"
)

func main() {
	// Example 1: Using Flat Index (default, good for small datasets)
	fmt.Println("=== Example 1: Flat Index ===")
	flatConfig := veclite.DefaultConfig()
	flatConfig.Dimension = 128
	flatConfig.DataPath = "./example_flat.db"
	flatConfig.IndexType = "flat"
	flatConfig.CacheCapacity = 1000 // Enable LRU cache with 1000 capacity

	flatDB, err := veclite.New(flatConfig)
	if err != nil {
		log.Fatalf("Failed to create VecLite: %v", err)
	}
	defer flatDB.Close()

	// Insert some vectors
	fmt.Println("Inserting 100 vectors into Flat index...")
	for i := 0; i < 100; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}

		if err := flatDB.Insert(uint64(i+1), vector); err != nil {
			log.Printf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Search for similar vectors
	fmt.Println("Searching for similar vectors...")
	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}

	results, err := flatDB.Search(query, 5)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	fmt.Println("\nTop 5 results (Flat index):")
	for i, result := range results {
		fmt.Printf("%d. ID: %d, Distance: %.4f\n", i+1, result.ID, result.Distance)
	}

	// Example 2: Using HNSW Index (better for large datasets)
	fmt.Println("\n=== Example 2: HNSW Index ===")
	hnswConfig := veclite.DefaultConfig()
	hnswConfig.Dimension = 128
	hnswConfig.DataPath = "./example_hnsw.db"
	hnswConfig.IndexType = "hnsw"
	hnswConfig.CacheCapacity = 1000 // Enable LRU cache
	// HNSW parameters (optional, defaults shown)
	hnswConfig.M = 16              // Maximum connections per node
	hnswConfig.EfConstruction = 64 // Search width during construction
	hnswConfig.EfSearch = 10       // Search width during query

	hnswDB, err := veclite.New(hnswConfig)
	if err != nil {
		log.Fatalf("Failed to create HNSW VecLite: %v", err)
	}
	defer hnswDB.Close()

	// Insert some vectors
	fmt.Println("Inserting 100 vectors into HNSW index...")
	for i := 0; i < 100; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}

		if err := hnswDB.Insert(uint64(i+1), vector); err != nil {
			log.Printf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Search for similar vectors
	fmt.Println("Searching for similar vectors...")
	results, err = hnswDB.Search(query, 5)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	fmt.Println("\nTop 5 results (HNSW index):")
	for i, result := range results {
		fmt.Printf("%d. ID: %d, Distance: %.4f\n", i+1, result.ID, result.Distance)
	}

	// Example 3: Reading a vector by ID
	fmt.Println("\n=== Example 3: Reading Vector by ID ===")
	vec, err := hnswDB.Get(1)
	if err != nil {
		log.Printf("Failed to read vector: %v", err)
	} else {
		fmt.Printf("Retrieved vector ID 1 (dimension: %d)\n", len(vec))
	}

	// Example 4: Database size
	fmt.Println("\n=== Example 4: Database Statistics ===")
	size := hnswDB.Size()
	fmt.Printf("Total vectors in database: %d\n", size)
}
