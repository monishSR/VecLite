package utils

import (
	"math"
	"testing"
)

func TestCandidateHeap_PushCandidate(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push candidates with different distances
	candidates := []Candidate{
		{ID: 1, Distance: 0.5},
		{ID: 2, Distance: 1.2},
		{ID: 3, Distance: 0.8},
		{ID: 4, Distance: 2.1},
		{ID: 5, Distance: 0.3},
	}

	for _, cand := range candidates {
		heap.PushCandidate(cand)
	}

	// Verify heap size
	if heap.Len() != len(candidates) {
		t.Errorf("Expected heap size %d, got %d", len(candidates), heap.Len())
	}

	// Verify heap property: worst (largest distance) should be at top
	worst := heap.Peek()
	if worst.Distance != 2.1 {
		t.Errorf("Expected worst distance 2.1, got %f", worst.Distance)
	}
	if worst.ID != 4 {
		t.Errorf("Expected worst ID 4, got %d", worst.ID)
	}
}

func TestCandidateHeap_PopCandidate(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push candidates in order
	candidates := []Candidate{
		{ID: 1, Distance: 0.5},
		{ID: 2, Distance: 1.2},
		{ID: 3, Distance: 2.8},
		{ID: 4, Distance: 0.3},
		{ID: 5, Distance: 1.5},
	}

	for _, cand := range candidates {
		heap.PushCandidate(cand)
	}

	// Pop should return worst candidates first (largest distance)
	expectedOrder := []float32{2.8, 1.5, 1.2, 0.5, 0.3}
	for i, expectedDist := range expectedOrder {
		popped := heap.PopCandidate()
		if math.Abs(float64(popped.Distance-expectedDist)) > 0.001 {
			t.Errorf("Pop %d: expected distance %f, got %f", i, expectedDist, popped.Distance)
		}
		if heap.Len() != len(expectedOrder)-i-1 {
			t.Errorf("After pop %d: expected heap size %d, got %d", i, len(expectedOrder)-i-1, heap.Len())
		}
	}

	// Heap should be empty
	if heap.Len() != 0 {
		t.Errorf("Expected empty heap, got size %d", heap.Len())
	}
}

func TestCandidateHeap_PushAndPop(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Test push-pop cycle
	heap.PushCandidate(Candidate{ID: 1, Distance: 1.0})
	heap.PushCandidate(Candidate{ID: 2, Distance: 2.0})
	heap.PushCandidate(Candidate{ID: 3, Distance: 0.5})

	// Pop worst (should be 2.0)
	worst := heap.PopCandidate()
	if worst.Distance != 2.0 || worst.ID != 2 {
		t.Errorf("Expected worst candidate {ID: 2, Distance: 2.0}, got {ID: %d, Distance: %f}", worst.ID, worst.Distance)
	}

	// Pop next worst (should be 1.0)
	next := heap.PopCandidate()
	if next.Distance != 1.0 || next.ID != 1 {
		t.Errorf("Expected next candidate {ID: 1, Distance: 1.0}, got {ID: %d, Distance: %f}", next.ID, next.Distance)
	}

	// Pop best (should be 0.5)
	best := heap.PopCandidate()
	if best.Distance != 0.5 || best.ID != 3 {
		t.Errorf("Expected best candidate {ID: 3, Distance: 0.5}, got {ID: %d, Distance: %f}", best.ID, best.Distance)
	}
}

func TestCandidateHeap_Peek(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Peek on empty heap should panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when peeking empty heap")
			}
		}()
		heap.Peek()
	}()

	// Push candidates
	heap.PushCandidate(Candidate{ID: 1, Distance: 1.5})
	heap.PushCandidate(Candidate{ID: 2, Distance: 0.8})
	heap.PushCandidate(Candidate{ID: 3, Distance: 2.2})

	// Peek should return worst without removing
	worst := heap.Peek()
	if worst.Distance != 2.2 || worst.ID != 3 {
		t.Errorf("Expected worst {ID: 3, Distance: 2.2}, got {ID: %d, Distance: %f}", worst.ID, worst.Distance)
	}

	// Heap size should be unchanged
	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3 after peek, got %d", heap.Len())
	}
}

func TestCandidateHeap_AddCandidate(t *testing.T) {
	heap := NewCandidateHeap(3) // Max size 3

	// Add candidates up to max size
	heap.AddCandidate(Candidate{ID: 1, Distance: 1.0}, 3)
	heap.AddCandidate(Candidate{ID: 2, Distance: 2.0}, 3)
	heap.AddCandidate(Candidate{ID: 3, Distance: 0.5}, 3)

	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3, got %d", heap.Len())
	}

	// Add a candidate better than worst (should replace worst)
	heap.AddCandidate(Candidate{ID: 4, Distance: 0.8}, 3)
	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3 after adding better candidate, got %d", heap.Len())
	}

	// Worst should now be 1.0 (not 2.0)
	worst := heap.Peek()
	if worst.Distance != 1.0 {
		t.Errorf("Expected worst distance 1.0, got %f", worst.Distance)
	}

	// Add a candidate worse than all (should be ignored)
	heap.AddCandidate(Candidate{ID: 5, Distance: 3.0}, 3)
	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3 after adding worse candidate, got %d", heap.Len())
	}
	worst = heap.Peek()
	if worst.Distance != 1.0 {
		t.Errorf("Expected worst distance still 1.0, got %f", worst.Distance)
	}
}

func TestCandidateHeap_ExtractTop(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push candidates
	candidates := []Candidate{
		{ID: 1, Distance: 0.3},
		{ID: 2, Distance: 1.5},
		{ID: 3, Distance: 0.8},
		{ID: 4, Distance: 2.2},
		{ID: 5, Distance: 0.5},
	}

	for _, cand := range candidates {
		heap.PushCandidate(cand)
	}

	// Extract top 3 (should be best 3: 0.3, 0.5, 0.8)
	top3 := heap.ExtractTop(3)
	if len(top3) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(top3))
	}

	expected := []float32{0.3, 0.5, 0.8}
	for i, cand := range top3 {
		if math.Abs(float64(cand.Distance-expected[i])) > 0.001 {
			t.Errorf("Top %d: expected distance %f, got %f", i, expected[i], cand.Distance)
		}
	}

	// Verify they're in best-to-worst order
	for i := 0; i < len(top3)-1; i++ {
		if top3[i].Distance > top3[i+1].Distance {
			t.Errorf("Candidates not in best-to-worst order: %f > %f", top3[i].Distance, top3[i+1].Distance)
		}
	}

	// Heap should have remaining candidates
	if heap.Len() != 2 {
		t.Errorf("Expected heap size 2 after extracting 3, got %d", heap.Len())
	}
}

func TestCandidateHeap_ExtractTop_MoreThanAvailable(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push only 2 candidates
	heap.PushCandidate(Candidate{ID: 1, Distance: 0.5})
	heap.PushCandidate(Candidate{ID: 2, Distance: 1.0})

	// Try to extract 5 (should only get 2)
	top := heap.ExtractTop(5)
	if len(top) != 2 {
		t.Errorf("Expected 2 candidates, got %d", len(top))
	}

	// Heap should be empty
	if heap.Len() != 0 {
		t.Errorf("Expected empty heap, got size %d", heap.Len())
	}
}

func TestCandidateHeap_ExtractTop_EmptyHeap(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Extract from empty heap
	top := heap.ExtractTop(5)
	if len(top) != 0 {
		t.Errorf("Expected empty result, got %d candidates", len(top))
	}
}

func TestCandidateHeap_MaxHeapProperty(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Add many candidates
	distances := []float32{0.1, 0.9, 0.3, 0.7, 0.2, 0.8, 0.4, 0.6, 0.5}
	for i, dist := range distances {
		heap.PushCandidate(Candidate{ID: uint64(i + 1), Distance: dist})
	}

	// Verify max-heap property: worst is always at top
	prevDist := float32(math.MaxFloat32)
	for heap.Len() > 0 {
		worst := heap.PopCandidate()
		if worst.Distance > prevDist {
			t.Errorf("Heap property violated: popped %f after %f", worst.Distance, prevDist)
		}
		prevDist = worst.Distance
	}
}

func TestCandidateHeap_LargeDataset(t *testing.T) {
	heap := NewCandidateHeap(1000)

	// Add 1000 candidates
	for i := 0; i < 1000; i++ {
		dist := float32(i) / 100.0 // Distances from 0.0 to 9.99
		heap.PushCandidate(Candidate{ID: uint64(i), Distance: dist})
	}

	if heap.Len() != 1000 {
		t.Errorf("Expected heap size 1000, got %d", heap.Len())
	}

	// Worst should be ~9.99
	worst := heap.Peek()
	if worst.Distance < 9.9 {
		t.Errorf("Expected worst distance ~9.99, got %f", worst.Distance)
	}

	// Extract top 10 (should be 0.0 to 0.09)
	top10 := heap.ExtractTop(10)
	if len(top10) != 10 {
		t.Errorf("Expected 10 candidates, got %d", len(top10))
	}

	// Verify they're the best (smallest distances)
	for i, cand := range top10 {
		expected := float32(i) / 100.0
		if math.Abs(float64(cand.Distance-expected)) > 0.01 {
			t.Errorf("Top %d: expected distance ~%f, got %f", i, expected, cand.Distance)
		}
	}
}

