package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/monishSR/veclite/pkg/veclite"
)

func main() {
	// Database file path
	dbPath := "./veclite_example.db"
	graphPath := dbPath + ".graph"

	// Clean up any existing database files from previous runs
	os.Remove(dbPath)
	os.Remove(graphPath)

	fmt.Println("=== VecLite Basic Example ===")

	// Step 1: Create and configure the database
	fmt.Println("1. Creating database...")
	config := veclite.DefaultConfig()
	config.Dimension = 128
	config.DataPath = dbPath
	config.IndexType = "hnsw" // Use HNSW for better performance
	config.CacheCapacity = 1000

	db, err := veclite.New(config)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Step 2: Insert vectors
	fmt.Println("2. Inserting vectors...")
	vectors := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		// Create a random vector
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector

		// Insert with ID = i+1
		if err := db.Insert(uint64(i+1), vector); err != nil {
			log.Fatalf("Failed to insert vector %d: %v", i+1, err)
		}
		fmt.Printf("   Inserted vector ID %d\n", i+1)
	}

	fmt.Printf("\n   Database size: %d vectors\n", db.Size())

	// Step 3: Search for similar vectors
	fmt.Println("\n3. Searching for similar vectors...")
	query := vectors[0] // Use the first vector as query
	results, err := db.Search(query, 5)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	fmt.Println("   Top 5 most similar vectors:")
	for i, result := range results {
		fmt.Printf("   %d. ID: %d, Distance: %.4f\n", i+1, result.ID, result.Distance)
	}

	// Step 4: Demonstrate persistence
	fmt.Println("\n4. Demonstrating persistence...")
	fmt.Println("   Closing database...")
	db.Close() // Close the database

	// Reopen the same database
	fmt.Println("   Reopening database...")
	db2, err := veclite.New(config)
	if err != nil {
		log.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	fmt.Printf("   Database size after reopen: %d vectors\n", db2.Size())

	// Search again to verify data is accessible
	fmt.Println("   Searching again with persisted data...")
	results2, err := db2.Search(query, 5)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	fmt.Println("   Top 5 results (from persisted database):")
	for i, result := range results2 {
		fmt.Printf("   %d. ID: %d, Distance: %.4f\n", i+1, result.ID, result.Distance)
	}

	// Step 5: Retrieve a specific vector
	fmt.Println("\n5. Retrieving a specific vector...")
	vec, err := db2.Get(5)
	if err != nil {
		log.Fatalf("Failed to retrieve vector: %v", err)
	}
	fmt.Printf("   Retrieved vector ID 5 (dimension: %d)\n", len(vec))

	fmt.Println("\n=== Example Complete ===")
	fmt.Println("Database file:", dbPath)
	fmt.Println("Graph file:", graphPath)
}

