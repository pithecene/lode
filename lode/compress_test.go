package lode

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"weak"

	"github.com/go-openapi/testify/v2/assert"
)

// zstdRoundTrip compresses payload with c into a buffer and decompresses it.
func zstdRoundTrip(t *testing.T, c Compressor, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := c.Compress(&buf)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := c.Decompress(&buf)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	return got
}

// The streams of one compressor reuse its encoders, and each stream
// decompresses to exactly its own bytes.
func TestZstd_ReusedEncoderKeepsStreamsApart(t *testing.T) {
	t.Parallel()
	c := Zstd()
	for i := range 3 {
		payload := []byte(strings.Repeat(fmt.Sprintf("stream %d ", i), 100*(i+1)))
		if got := zstdRoundTrip(t, c, payload); !bytes.Equal(got, payload) {
			t.Fatalf("stream %d decompressed to %d bytes, want %d", i, len(got), len(payload))
		}
	}
}

// After Close, the writer's encoder may compress another stream, so the
// closed writer refuses writes, and a second Close does nothing.
func TestZstd_ClosedWriterRefusesWrites(t *testing.T) {
	t.Parallel()
	w, err := Zstd().Compress(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("late")); !errors.Is(err, errZstdWriterClosed) {
		t.Errorf("write after close = %v, want errZstdWriterClosed", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second close = %v, want nil", err)
	}
}

func TestZstd_ClosedEncoderReleasesSink(t *testing.T) {
	t.Parallel()
	sink := new(bytes.Buffer)
	sinkRef := weak.Make(sink)
	w, err := Zstd().Compress(sink)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the encoder alive even if the pool drops it during collection.
	enc := w.(*zstdWriter).enc
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if sinkRef.Value() != nil {
		t.Error("closed encoder still holds its output writer")
	}
	runtime.KeepAlive(enc)
}

// failingWriter refuses every write.
type failingWriter struct{}

var errSinkRefused = errors.New("sink refused")

func (failingWriter) Write([]byte) (int, error) { return 0, errSinkRefused }

// A stream whose Close fails returns the error and does not return its
// encoder, so the next stream of the compressor is unaffected.
func TestZstd_FailedCloseLeavesLaterStreamsIntact(t *testing.T) {
	t.Parallel()
	c := Zstd()
	w, err := c.Compress(failingWriter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, errSinkRefused) {
		t.Fatalf("close = %v, want the sink's error", err)
	}
	payload := []byte(strings.Repeat("after the failure ", 50))
	if got := zstdRoundTrip(t, c, payload); !bytes.Equal(got, payload) {
		t.Fatalf("later stream decompressed to %d bytes, want %d", len(got), len(payload))
	}
}

// Streams of one compressor on concurrent goroutines each round-trip their
// own bytes: an encoder serves one stream at a time.
func TestZstd_ConcurrentStreamsRoundTrip(t *testing.T) {
	t.Parallel()
	c := Zstd()
	const streams = 8
	errs := make([]error, streams)
	assert.NoGoRoutineLeak(t, func() {
		var wg sync.WaitGroup
		for i := range streams {
			wg.Go(func() {
				payload := []byte(strings.Repeat(fmt.Sprintf("goroutine %d ", i), 200))
				var buf bytes.Buffer
				w, err := c.Compress(&buf)
				if err != nil {
					errs[i] = err
					return
				}
				if _, err := w.Write(payload); err != nil {
					errs[i] = err
					return
				}
				if err := w.Close(); err != nil {
					errs[i] = err
					return
				}
				r, err := c.Decompress(&buf)
				if err != nil {
					errs[i] = err
					return
				}
				defer func() { errs[i] = errors.Join(errs[i], r.Close()) }()
				got, err := io.ReadAll(r)
				if err == nil && !bytes.Equal(got, payload) {
					err = fmt.Errorf("goroutine %d decompressed to %d bytes, want %d", i, len(got), len(payload))
				}
				errs[i] = err
			})
		}
		wg.Wait()
	})
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
}
