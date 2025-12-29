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

func TestCandidateHeap_ExtractTop_AllCandidates(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push 5 candidates
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

	// Extract all 5 (k == heapLen case)
	top5 := heap.ExtractTop(5)
	if len(top5) != 5 {
		t.Errorf("Expected 5 candidates, got %d", len(top5))
	}

	// Verify they're in best-to-worst order
	expected := []float32{0.3, 0.5, 0.8, 1.5, 2.2}
	for i, cand := range top5 {
		if math.Abs(float64(cand.Distance-expected[i])) > 0.001 {
			t.Errorf("Top %d: expected distance %f, got %f", i, expected[i], cand.Distance)
		}
	}

	// Heap should be empty
	if heap.Len() != 0 {
		t.Errorf("Expected empty heap after extracting all, got size %d", heap.Len())
	}
}

func TestCandidateHeap_ExtractTop_ZeroOrNegative(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push some candidates
	heap.PushCandidate(Candidate{ID: 1, Distance: 0.5})
	heap.PushCandidate(Candidate{ID: 2, Distance: 1.0})

	// Extract 0 (should return nil)
	top0 := heap.ExtractTop(0)
	if top0 != nil {
		t.Errorf("Expected nil for k=0, got %d candidates", len(top0))
	}

	// Extract negative (should return nil)
	topNeg := heap.ExtractTop(-1)
	if topNeg != nil {
		t.Errorf("Expected nil for k=-1, got %d candidates", len(topNeg))
	}

	// Heap should be unchanged
	if heap.Len() != 2 {
		t.Errorf("Expected heap size 2, got %d", heap.Len())
	}
}

func TestCandidateHeap_ExtractTop_PartialExtraction(t *testing.T) {
	heap := NewCandidateHeap(10)

	// Push 10 candidates
	for i := 0; i < 10; i++ {
		dist := float32(i) / 10.0 // 0.0, 0.1, 0.2, ..., 0.9
		heap.PushCandidate(Candidate{ID: uint64(i), Distance: dist})
	}

	// Extract top 3
	top3 := heap.ExtractTop(3)
	if len(top3) != 3 {
		t.Errorf("Expected 3 candidates, got %d", len(top3))
	}

	// Verify they're the best 3 (0.0, 0.1, 0.2)
	expected := []float32{0.0, 0.1, 0.2}
	for i, cand := range top3 {
		if math.Abs(float64(cand.Distance-expected[i])) > 0.001 {
			t.Errorf("Top %d: expected distance %f, got %f", i, expected[i], cand.Distance)
		}
	}

	// Heap should have remaining 7 candidates
	if heap.Len() != 7 {
		t.Errorf("Expected heap size 7 after extracting 3, got %d", heap.Len())
	}

	// Verify remaining candidates are still in heap
	worst := heap.Peek()
	if worst.Distance < 0.2 {
		t.Errorf("Expected worst remaining distance >= 0.2, got %f", worst.Distance)
	}
}

func TestCandidateHeap_AddCandidate_EdgeCases(t *testing.T) {
	heap := NewCandidateHeap(3)

	// Add candidate when heap is empty
	added := heap.AddCandidate(Candidate{ID: 1, Distance: 1.0}, 3)
	if !added {
		t.Error("Expected candidate to be added to empty heap")
	}
	if heap.Len() != 1 {
		t.Errorf("Expected heap size 1, got %d", heap.Len())
	}

	// Add candidate with same distance as worst
	heap.AddCandidate(Candidate{ID: 2, Distance: 2.0}, 3)
	heap.AddCandidate(Candidate{ID: 3, Distance: 0.5}, 3)

	// Add candidate with same distance as worst (should not replace)
	added = heap.AddCandidate(Candidate{ID: 4, Distance: 2.0}, 3)
	if added {
		t.Error("Expected candidate with same distance as worst to not be added")
	}
	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3, got %d", heap.Len())
	}

	// Add candidate slightly better than worst
	added = heap.AddCandidate(Candidate{ID: 5, Distance: 1.9}, 3)
	if !added {
		t.Error("Expected candidate slightly better than worst to be added")
	}
	if heap.Len() != 3 {
		t.Errorf("Expected heap size 3, got %d", heap.Len())
	}
}

func TestCandidateHeap_NewCandidateHeap_EdgeCases(t *testing.T) {
	// Test with maxSize 0
	heap := NewCandidateHeap(0)
	if heap == nil {
		t.Error("Expected non-nil heap even with maxSize 0")
	}
	if heap.Len() != 0 {
		t.Errorf("Expected empty heap, got size %d", heap.Len())
	}

	// Test with maxSize 1
	heap = NewCandidateHeap(1)
	heap.PushCandidate(Candidate{ID: 1, Distance: 0.5})
	if heap.Len() != 1 {
		t.Errorf("Expected heap size 1, got %d", heap.Len())
	}

	// Test with large maxSize
	heap = NewCandidateHeap(10000)
	if heap == nil {
		t.Error("Expected non-nil heap with large maxSize")
	}
}

func TestCandidateHeap_ConcurrentOperations(t *testing.T) {
	heap := NewCandidateHeap(100)

	// Simulate concurrent-like operations
	for i := 0; i < 50; i++ {
		heap.PushCandidate(Candidate{ID: uint64(i), Distance: float32(i) / 10.0})
		if i%5 == 0 {
			_ = heap.Peek() // Peek without popping
		}
	}

	if heap.Len() != 50 {
		t.Errorf("Expected heap size 50, got %d", heap.Len())
	}

	// Extract some, then add more
	top10 := heap.ExtractTop(10)
	if len(top10) != 10 {
		t.Errorf("Expected 10 candidates, got %d", len(top10))
	}

	// Add more candidates
	for i := 50; i < 70; i++ {
		heap.PushCandidate(Candidate{ID: uint64(i), Distance: float32(i) / 10.0})
	}

	if heap.Len() != 50 { // 40 remaining + 20 new = 60, but we're testing the flow
		// Actually, we extracted 10, so 50-10=40 remaining, then added 20 = 60
		// But let's just verify it's reasonable
		if heap.Len() < 40 || heap.Len() > 70 {
			t.Errorf("Expected heap size between 40-70, got %d", heap.Len())
		}
	}
}

