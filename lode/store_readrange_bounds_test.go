package lode

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

// ReadRange boundary tests shared by the filesystem and memory stores
// (CONTRACT_STORAGE §ReadRange). The existing tests probe clearly-inside and
// clearly-outside values; these sit exactly on the edges the contract names.
//
// Known equivalent mutants left alive: in memoryStore.ReadRange,
// `offset >= len(data)` → `>` and `end > len(data)` → `>=` (at equality both
// branches yield the same empty or full slice); in fsStore.ReadRange, the
// `available` arithmetic and `available <= 0` → `<` (only the buffer size
// differs; ReadAt still returns exactly the bytes present).

func assertReadRangeBounds(t *testing.T, store Store) {
	t.Helper()
	ctx := t.Context()
	content := []byte("hello")
	if err := store.Put(ctx, "test.txt", bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	// offset+length == MaxInt64 does not overflow; it is a valid (empty) range.
	data, err := rangesOf(t, store).ReadRange(ctx, "test.txt", math.MaxInt64-10, 10)
	if err != nil {
		t.Fatalf("exact MaxInt64 bound: unexpected error %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("exact MaxInt64 bound: got %d bytes, want empty", len(data))
	}

	// offset+length == MaxInt64+1 overflows.
	if _, err := rangesOf(t, store).ReadRange(ctx, "test.txt", math.MaxInt64-10, 11); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("one past MaxInt64: got %v, want ErrInvalidPath", err)
	}

	// length == maxReadRangeLength is the largest accepted length and must
	// return the available bytes without allocating the requested size.
	data, err = rangesOf(t, store).ReadRange(ctx, "test.txt", 0, maxReadRangeLength)
	if err != nil {
		t.Fatalf("max length: unexpected error %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Fatalf("max length: got %q, want %q", data, content)
	}

	// A large valid length past EOF returns the available bytes. Derived from
	// the platform limit so the case stays valid where int is 32 bits.
	data, err = rangesOf(t, store).ReadRange(ctx, "test.txt", 3, maxReadRangeLength/2)
	if err != nil {
		t.Fatalf("large length: unexpected error %v", err)
	}
	if string(data) != "lo" {
		t.Fatalf("large length: got %q, want %q", data, "lo")
	}

	// Zero length is valid and empty, not ErrInvalidPath.
	data, err = rangesOf(t, store).ReadRange(ctx, "test.txt", 1, 0)
	if err != nil || len(data) != 0 {
		t.Fatalf("zero length: got %d bytes, %v; want empty, nil", len(data), err)
	}

	// offset exactly at EOF is empty, not an error.
	data, err = rangesOf(t, store).ReadRange(ctx, "test.txt", int64(len(content)), 1)
	if err != nil {
		t.Fatalf("offset at EOF: unexpected error %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("offset at EOF: got %d bytes, want empty", len(data))
	}

	// Range ending exactly at EOF returns everything requested.
	data, err = rangesOf(t, store).ReadRange(ctx, "test.txt", 2, 3)
	if err != nil {
		t.Fatalf("range to EOF: unexpected error %v", err)
	}
	if string(data) != "llo" {
		t.Fatalf("range to EOF: got %q, want %q", data, "llo")
	}
}
