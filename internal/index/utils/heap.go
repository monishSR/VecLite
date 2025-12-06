package utils

import (
	"container/heap"
)

// Candidate represents a potential nearest neighbor during search or insert
type Candidate struct {
	ID       uint64  // Vector/node ID
	Distance float32 // Distance to query vector
}

// CandidateHeap is a max-heap (worst distance at top for easy removal)
// Implements heap.Interface for use with container/heap
type CandidateHeap []Candidate

// Len returns the number of elements in the heap
func (h CandidateHeap) Len() int { return len(h) }

// Less defines the ordering: larger distance = higher priority (max-heap)
// This allows us to easily remove the worst candidate (at index 0)
func (h CandidateHeap) Less(i, j int) bool {
	return h[i].Distance > h[j].Distance // Max-heap: larger distance = higher priority
}

// Swap swaps two elements in the heap
func (h CandidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap (called by heap.Push)
func (h *CandidateHeap) Push(x interface{}) {
	*h = append(*h, x.(Candidate))
}

// Pop removes and returns the element with highest priority (worst distance)
// Called by heap.Pop - returns the last element (heap.Pop swaps first with last first)
func (h *CandidateHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewCandidateHeap creates a new candidate heap with the specified max size
func NewCandidateHeap(maxSize int) *CandidateHeap {
	h := make(CandidateHeap, 0, maxSize)
	heap.Init(&h)
	return &h
}

// Peek returns the worst candidate (largest distance) without removing it
// Panics if heap is empty
func (h *CandidateHeap) Peek() Candidate {
	if h.Len() == 0 {
		panic("heap is empty")
	}
	return (*h)[0]
}

// ExtractTop extracts the top k candidates (best ones, smallest distance)
// Returns them in order from best to worst
// Optimized: Only extract what we need, avoid full heap reconstruction
func (h *CandidateHeap) ExtractTop(k int) []Candidate {
	if k <= 0 {
		return nil
	}

	heapLen := h.Len()
	if k > heapLen {
		k = heapLen
	}

	// If k equals heap size, just pop all and reverse
	if k == heapLen {
		result := make([]Candidate, heapLen)
		for i := heapLen - 1; i >= 0; i-- {
			result[i] = h.PopCandidate()
		}
		return result
	}

	// For k < heapLen, we need to be more careful
	// Extract all, take best k, put rest back
	all := make([]Candidate, 0, heapLen)
	for h.Len() > 0 {
		all = append(all, h.PopCandidate())
	}

	// Take the last k (which are the best, since we popped worst-first)
	start := len(all) - k
	result := make([]Candidate, k)
	copy(result, all[start:])

	// Reverse to get best first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	// Put remaining candidates back into heap (only if needed)
	if start > 0 {
		for i := 0; i < start; i++ {
			h.PushCandidate(all[i])
		}
	}

	return result
}

// PushCandidate adds a candidate to the heap and maintains heap property
// This is a convenience method that wraps heap.Push
func (h *CandidateHeap) PushCandidate(cand Candidate) {
	heap.Push(h, cand)
}

// PopCandidate removes and returns the worst candidate (largest distance)
// This is a convenience method that wraps heap.Pop
func (h *CandidateHeap) PopCandidate() Candidate {
	return heap.Pop(h).(Candidate)
}

// AddCandidate adds a candidate to the heap if it's better than the worst
// or if the heap hasn't reached maxSize yet
// Returns true if candidate was added, false otherwise
func (h *CandidateHeap) AddCandidate(cand Candidate, maxSize int) bool {
	if h.Len() < maxSize {
		h.PushCandidate(cand)
		return true
	}
	// Only peek if heap is full (avoid expensive peek when not needed)
	if cand.Distance < h.Peek().Distance {
		// New candidate is better than worst, replace it
		h.PopCandidate()
		h.PushCandidate(cand)
		return true
	}
	// Candidate is worse than all in heap, ignore it
	return false
}

