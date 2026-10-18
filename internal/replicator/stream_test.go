package replicator

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestExactReader(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 100)

	if _, err := io.ReadAll(&exactReader{r: bytes.NewReader(data), left: 100}); err != nil {
		t.Fatalf("exact body: %v", err)
	}
	if _, err := io.ReadAll(&exactReader{r: bytes.NewReader(data), left: 150}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short body: got %v, want ErrUnexpectedEOF", err)
	}
}

func TestSetStreamThresholdDefault(t *testing.T) {
	r := &Replicator{}
	r.SetStreamThreshold(0)
	if r.streamThreshold != DefaultStreamThreshold {
		t.Fatalf("got %d", r.streamThreshold)
	}
}
