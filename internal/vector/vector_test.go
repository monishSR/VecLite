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
