package utils

import "fmt"

// FailingWriter is a helper type to simulate write failures in tests.
// It implements io.Writer and can be configured to fail after a certain
// number of bytes have been written.
type FailingWriter struct {
	BytesWritten int
	FailAfter    int
	ShouldFail   bool
}

// Write implements io.Writer interface.
// It will fail if ShouldFail is true, or after FailAfter bytes have been written.
func (fw *FailingWriter) Write(p []byte) (int, error) {
	if fw.ShouldFail {
		return 0, fmt.Errorf("simulated write error")
	}
	if fw.BytesWritten >= fw.FailAfter {
		return 0, fmt.Errorf("simulated write error after %d bytes", fw.BytesWritten)
	}
	fw.BytesWritten += len(p)
	return len(p), nil
}

