package utils

import (
	"testing"
)

func TestFailingWriter_ShouldFail(t *testing.T) {
	fw := &FailingWriter{
		ShouldFail: true,
	}

	// Write should fail immediately
	data := []byte("test data")
	n, err := fw.Write(data)
	if err == nil {
		t.Error("Expected error when ShouldFail is true")
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written, got %d", n)
	}
	if fw.BytesWritten != 0 {
		t.Errorf("Expected BytesWritten 0, got %d", fw.BytesWritten)
	}
}

func TestFailingWriter_FailAfter(t *testing.T) {
	fw := &FailingWriter{
		FailAfter: 10,
		ShouldFail: false,
	}

	// Write 5 bytes (should succeed)
	data1 := []byte("12345")
	n, err := fw.Write(data1)
	if err != nil {
		t.Errorf("Expected no error for first write, got: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}
	if fw.BytesWritten != 5 {
		t.Errorf("Expected BytesWritten 5, got %d", fw.BytesWritten)
	}

	// Write 5 more bytes (should succeed, total = 10)
	data2 := []byte("67890")
	n, err = fw.Write(data2)
	if err != nil {
		t.Errorf("Expected no error for second write, got: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}
	if fw.BytesWritten != 10 {
		t.Errorf("Expected BytesWritten 10, got %d", fw.BytesWritten)
	}

	// Write 1 more byte (should fail, BytesWritten >= FailAfter)
	data3 := []byte("x")
	n, err = fw.Write(data3)
	if err == nil {
		t.Error("Expected error when BytesWritten >= FailAfter")
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written on failure, got %d", n)
	}
	if fw.BytesWritten != 10 {
		t.Errorf("Expected BytesWritten to remain 10, got %d", fw.BytesWritten)
	}
}

func TestFailingWriter_FailAfter_ExactMatch(t *testing.T) {
	fw := &FailingWriter{
		FailAfter: 10,
		ShouldFail: false,
	}

	// Write exactly 10 bytes (should succeed, then next write fails)
	data := []byte("1234567890")
	n, err := fw.Write(data)
	if err != nil {
		t.Errorf("Expected no error for exact match write, got: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected 10 bytes written, got %d", n)
	}
	if fw.BytesWritten != 10 {
		t.Errorf("Expected BytesWritten 10, got %d", fw.BytesWritten)
	}

	// Next write should fail
	data2 := []byte("x")
	n, err = fw.Write(data2)
	if err == nil {
		t.Error("Expected error when BytesWritten >= FailAfter")
	}
}

func TestFailingWriter_FailAfter_LargeWrite(t *testing.T) {
	fw := &FailingWriter{
		FailAfter: 5,
		ShouldFail: false,
	}

	// Write 10 bytes when FailAfter is 5
	// The check happens BEFORE writing, so if BytesWritten (0) < FailAfter (5),
	// it will write all 10 bytes and update BytesWritten to 10
	data := []byte("1234567890")
	n, err := fw.Write(data)
	if err != nil {
		t.Errorf("Expected no error (check happens before write), got: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected 10 bytes written, got %d", n)
	}
	if fw.BytesWritten != 10 {
		t.Errorf("Expected BytesWritten 10, got %d", fw.BytesWritten)
	}

	// Now the next write should fail (BytesWritten 10 >= FailAfter 5)
	data2 := []byte("x")
	n, err = fw.Write(data2)
	if err == nil {
		t.Error("Expected error when BytesWritten >= FailAfter")
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written on failure, got %d", n)
	}
	if fw.BytesWritten != 10 {
		t.Errorf("Expected BytesWritten to remain 10, got %d", fw.BytesWritten)
	}
}

func TestFailingWriter_NoFailure(t *testing.T) {
	fw := &FailingWriter{
		FailAfter:    100,
		ShouldFail:   false,
		BytesWritten: 0,
	}

	// Write multiple times, should all succeed
	data1 := []byte("test")
	n, err := fw.Write(data1)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if n != 4 {
		t.Errorf("Expected 4 bytes written, got %d", n)
	}

	data2 := []byte(" more data")
	n, err = fw.Write(data2)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected 10 bytes written, got %d", n)
	}

	if fw.BytesWritten != 14 {
		t.Errorf("Expected BytesWritten 14, got %d", fw.BytesWritten)
	}
}

func TestFailingWriter_EmptyWrite(t *testing.T) {
	fw := &FailingWriter{
		FailAfter:  10,
		ShouldFail: false,
	}

	// Write empty slice
	data := []byte{}
	n, err := fw.Write(data)
	if err != nil {
		t.Errorf("Expected no error for empty write, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written, got %d", n)
	}
	if fw.BytesWritten != 0 {
		t.Errorf("Expected BytesWritten 0, got %d", fw.BytesWritten)
	}
}

func TestFailingWriter_ShouldFailTakesPrecedence(t *testing.T) {
	fw := &FailingWriter{
		FailAfter:  100,
		ShouldFail: true, // Should fail even though FailAfter is high
	}

	data := []byte("test")
	n, err := fw.Write(data)
	if err == nil {
		t.Error("Expected error when ShouldFail is true, even with high FailAfter")
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes written, got %d", n)
	}
}

func TestFailingWriter_MultipleWrites(t *testing.T) {
	fw := &FailingWriter{
		FailAfter:  20,
		ShouldFail: false,
	}

	// Write multiple times
	writes := [][]byte{
		[]byte("hello"),
		[]byte(" "),
		[]byte("world"),
		[]byte("!"),
	}

	totalExpected := 0
	for i, data := range writes {
		_, err := fw.Write(data)
		if err != nil {
			t.Errorf("Write %d failed unexpectedly: %v", i, err)
		}
		totalExpected += len(data)
		if fw.BytesWritten != totalExpected {
			t.Errorf("After write %d: expected BytesWritten %d, got %d", i, totalExpected, fw.BytesWritten)
		}
	}

	// Next write should fail (total would be 12, but we're testing the logic)
	// Actually, 12 < 20, so it should succeed
	data := []byte("more")
	_, err := fw.Write(data)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	// Now BytesWritten is 16

	// Write 4 more bytes (16 + 4 = 20, should succeed)
	data2 := []byte("1234")
	_, err = fw.Write(data2)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	// Now BytesWritten is 20

	// Next write should fail (BytesWritten >= FailAfter)
	data3 := []byte("x")
	_, err = fw.Write(data3)
	if err == nil {
		t.Error("Expected error when BytesWritten >= FailAfter")
	}
}

