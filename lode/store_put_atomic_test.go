package lode

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// Put is all-or-nothing (CONTRACT_STORAGE §Put): a reader that fails part way
// through must leave the path as it was, so the caller never has to clean up
// after a failed Put or guess whether an object at the path is its own.

var errReaderBoom = errors.New("reader failure")

// failingReader yields prefix, then fails.
type failingReader struct {
	prefix *bytes.Reader
}

func newFailingReader(prefix string) *failingReader {
	return &failingReader{prefix: bytes.NewReader([]byte(prefix))}
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.prefix.Len() > 0 {
		return f.prefix.Read(p)
	}
	return 0, errReaderBoom
}

func assertPutAllOrNothing(t *testing.T, store Store) {
	t.Helper()
	ctx := t.Context()

	err := store.Put(ctx, "obj", newFailingReader("partial"))
	if !errors.Is(err, errReaderBoom) {
		t.Fatalf("Put with failing reader: got %v, want wrapped reader error", err)
	}
	exists, err := store.Exists(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed Put left an object behind")
	}

	// The path is free again: a subsequent Put succeeds and reads back whole.
	if err := store.Put(ctx, "obj", bytes.NewReader([]byte("complete"))); err != nil {
		t.Fatalf("Put after failed Put: %v", err)
	}
	rc, err := store.Get(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	defer closer(rc)()
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != "complete" {
		t.Fatalf("Get after retry: got %q, %v; want \"complete\"", got, err)
	}

	// An existing object is untouched by a failed Put at the same path.
	err = store.Put(ctx, "obj", newFailingReader("x"))
	if err == nil {
		t.Fatal("Put over an existing object must fail")
	}
	rc, err = store.Get(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	defer closer(rc)()
	if got, _ := io.ReadAll(rc); string(got) != "complete" {
		t.Fatalf("existing object altered by failed Put: %q", got)
	}
}

func TestFSStore_Put_AllOrNothing(t *testing.T) {
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	assertPutAllOrNothing(t, store)
}

func TestMemoryStore_Put_AllOrNothing(t *testing.T) {
	assertPutAllOrNothing(t, NewMemory())
}
