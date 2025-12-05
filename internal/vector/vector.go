package vector

import (
	"math"
)

// DotProduct calculates the dot product of two vectors
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// L2Distance calculates the L2 (Euclidean) distance between two vectors
func L2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		return math.MaxFloat32
	}

	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// CosineDistance calculates the cosine distance between two vectors
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return 1.0
	}

	dot := DotProduct(a, b)
	magA := Magnitude(a)
	magB := Magnitude(b)

	if magA == 0 || magB == 0 {
		return 1.0
	}

	// Cosine similarity = dot / (magA * magB)
	// Cosine distance = 1 - similarity
	return 1.0 - (dot / (magA * magB))
}

// Magnitude calculates the magnitude (L2 norm) of a vector
func Magnitude(v []float32) float32 {
	var sum float32
	for _, val := range v {
		sum += val * val
	}
	return float32(math.Sqrt(float64(sum)))
}

// Normalize normalizes a vector to unit length
func Normalize(v []float32) []float32 {
	mag := Magnitude(v)
	if mag == 0 {
		return v
	}

	normalized := make([]float32, len(v))
	for i := range v {
		normalized[i] = v[i] / mag
	}
	return normalized
}

// Validate checks if a vector has the expected dimension
func Validate(v []float32, dimension int) bool {
	return len(v) == dimension
}
