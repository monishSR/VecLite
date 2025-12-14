package vector

import (
	"math"
	"testing"
)

func TestDotProduct(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}

	expected := float32(1.0*4.0 + 2.0*5.0 + 3.0*6.0)
	result := DotProduct(a, b)

	if result != expected {
		t.Errorf("Expected %f, got %f", expected, result)
	}
}

func TestL2Distance(t *testing.T) {
	a := []float32{0.0, 0.0}
	b := []float32{3.0, 4.0}

	expected := float32(5.0) // 3-4-5 triangle
	result := L2Distance(a, b)

	if math.Abs(float64(result-expected)) > 0.001 {
		t.Errorf("Expected %f, got %f", expected, result)
	}
}

func TestMagnitude(t *testing.T) {
	v := []float32{3.0, 4.0}
	expected := float32(5.0)
	result := Magnitude(v)

	if math.Abs(float64(result-expected)) > 0.001 {
		t.Errorf("Expected %f, got %f", expected, result)
	}
}

func TestNormalize(t *testing.T) {
	v := []float32{3.0, 4.0}
	normalized := Normalize(v)

	mag := Magnitude(normalized)
	if math.Abs(float64(mag-1.0)) > 0.001 {
		t.Errorf("Expected magnitude 1.0, got %f", mag)
	}
}

func TestCosineDistance(t *testing.T) {
	// Test identical vectors (should have distance 0)
	a := []float32{1.0, 0.0}
	b := []float32{1.0, 0.0}
	result := CosineDistance(a, b)
	if math.Abs(float64(result)) > 0.001 {
		t.Errorf("Expected cosine distance 0 for identical vectors, got %f", result)
	}

	// Test orthogonal vectors (should have distance 1)
	a = []float32{1.0, 0.0}
	b = []float32{0.0, 1.0}
	result = CosineDistance(a, b)
	if math.Abs(float64(result-1.0)) > 0.001 {
		t.Errorf("Expected cosine distance 1 for orthogonal vectors, got %f", result)
	}

	// Test different length vectors (should return 1.0)
	a = []float32{1.0, 2.0}
	b = []float32{1.0}
	result = CosineDistance(a, b)
	if math.Abs(float64(result-1.0)) > 0.001 {
		t.Errorf("Expected cosine distance 1.0 for different length vectors, got %f", result)
	}

	// Test zero magnitude vectors (should return 1.0)
	a = []float32{0.0, 0.0}
	b = []float32{1.0, 0.0}
	result = CosineDistance(a, b)
	if math.Abs(float64(result-1.0)) > 0.001 {
		t.Errorf("Expected cosine distance 1.0 for zero magnitude vector, got %f", result)
	}
}

func TestValidate(t *testing.T) {
	// Test valid vector
	v := []float32{1.0, 2.0, 3.0}
	if !Validate(v, 3) {
		t.Error("Expected Validate to return true for valid vector")
	}

	// Test invalid dimension
	if Validate(v, 2) {
		t.Error("Expected Validate to return false for wrong dimension")
	}

	// Test empty vector
	empty := []float32{}
	if !Validate(empty, 0) {
		t.Error("Expected Validate to return true for empty vector with dimension 0")
	}
	if Validate(empty, 1) {
		t.Error("Expected Validate to return false for empty vector with dimension 1")
	}
}
