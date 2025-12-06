package hnsw

import (
	"container/heap"
)

// candidate represents a potential nearest neighbor during search or insert
type candidate struct {
	id       uint64  // Vector/node ID
	distance float32 // Distance to query vector
}

// candidateHeap is a max-heap (worst distance at top for easy removal)
// Implements heap.Interface for use with container/heap
type candidateHeap []candidate

// Len returns the number of elements in the heap
func (h candidateHeap) Len() int { return len(h) }

// Less defines the ordering: larger distance = higher priority (max-heap)
// This allows us to easily remove the worst candidate (at index 0)
// YES - it uses distance value for sorting!
func (h candidateHeap) Less(i, j int) bool {
	return h[i].distance > h[j].distance // Max-heap: larger distance = higher priority
}

// Swap swaps two elements in the heap
func (h candidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap (called by heap.Push)
func (h *candidateHeap) Push(x interface{}) {
	*h = append(*h, x.(candidate))
}

// Pop removes and returns the element with highest priority (worst distance)
// Called by heap.Pop - returns the last element (heap.Pop swaps first with last first)
func (h *candidateHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// newCandidateHeap creates a new candidate heap with the specified max size
func newCandidateHeap(maxSize int) *candidateHeap {
	h := make(candidateHeap, 0, maxSize)
	heap.Init(&h)
	return &h
}

// Peek returns the worst candidate (largest distance) without removing it
// Panics if heap is empty
func (h *candidateHeap) Peek() candidate {
	if h.Len() == 0 {
		panic("heap is empty")
	}
	return (*h)[0]
}

// ExtractTop extracts the top k candidates (best ones, smallest distance)
// Returns them in order from best to worst
// Since we have a max-heap (worst at top), we need to pop all and take the best k
// Then put the remaining candidates back into the heap
func (h *candidateHeap) ExtractTop(k int) []candidate {
	if k <= 0 {
		return nil
	}

	// Pop all candidates (worst to best order)
	all := make([]candidate, 0, h.Len())
	for h.Len() > 0 {
		all = append(all, h.PopCandidate())
	}

	// Take the last k (which are the best, since we popped worst-first)
	if k > len(all) {
		k = len(all)
	}
	start := len(all) - k
	result := make([]candidate, k)
	copy(result, all[start:])

	// Reverse to get best first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	// Put remaining candidates back into heap
	for i := 0; i < start; i++ {
		h.PushCandidate(all[i])
	}

	return result
}

// PushCandidate adds a candidate to the heap and maintains heap property
// This is a convenience method that wraps heap.Push
func (h *candidateHeap) PushCandidate(cand candidate) {
	heap.Push(h, cand)
}

// PopCandidate removes and returns the worst candidate (largest distance)
// This is a convenience method that wraps heap.Pop
func (h *candidateHeap) PopCandidate() candidate {
	return heap.Pop(h).(candidate)
}

// AddCandidate adds a candidate to the heap if it's better than the worst
// or if the heap hasn't reached maxSize yet
func (h *candidateHeap) AddCandidate(cand candidate, maxSize int) {
	if h.Len() < maxSize {
		h.PushCandidate(cand)
	} else if cand.distance < h.Peek().distance {
		// New candidate is better than worst, replace it
		h.PopCandidate()
		h.PushCandidate(cand)
	}
	// Otherwise, ignore this candidate (it's worse than all in heap)
}

