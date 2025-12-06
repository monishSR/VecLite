package types

import "errors"

// SearchResult represents a search result with ID, distance, and vector
type SearchResult struct {
	ID       uint64
	Distance float32
	Vector   []float32
}

// Common errors used by all index implementations
var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
	ErrInvalidK          = errors.New("k must be greater than 0")
)

