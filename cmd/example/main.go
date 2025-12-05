package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/msr23/veclite/pkg/veclite"
)

func main() {
	// Create a new VecLite instance
	config := veclite.DefaultConfig()
	config.Dimension = 128
	config.DataPath = "./example.db"

	db, err := veclite.New(config)
	if err != nil {
		log.Fatalf("Failed to create VecLite: %v", err)
	}
	defer db.Close()

	// Generate some random vectors
	fmt.Println("Inserting vectors...")
	for i := 0; i < 100; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}

		if err := db.Insert(uint64(i), vector); err != nil {
			log.Printf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Search for similar vectors
	fmt.Println("\nSearching for similar vectors...")
	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}

	results, err := db.Search(query, 5)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	fmt.Println("\nTop 5 results:")
	for i, result := range results {
		fmt.Printf("%d. ID: %d, Distance: %.4f\n", i+1, result.ID, result.Distance)
	}
}
