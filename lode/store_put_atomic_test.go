package lode

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

// A failed Put leaves no object at its path and nothing in a listing, even
// when the directory turns read-only mid-write: the object is published only
// after it is complete, and a leftover temporary file is never listed.
func TestFSStore_Put_FailureLeavesNothingVisible(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not bind root")
	}
	if runtime.GOOS == "windows" {
		// Windows ignores a directory's read-only mode when deleting the files
		// in it, so the removal this test needs to fail succeeds there.
		t.Skip("a read-only directory does not prevent deletion on Windows")
	}
	root := t.TempDir()
	store, err := NewFS(root)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "locked")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r := &lockingReader{dir: dir, inner: newFailingReader("partial")}
	err = store.Put(t.Context(), "locked/obj", r)
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if !errors.Is(err, errReaderBoom) {
		t.Fatalf("got %v, want the reader error in the chain", err)
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("got %v, want the failure to remove the temporary file in the chain", err)
	}
	if _, err := store.Get(t.Context(), "locked/obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after a failed Put = %v, want ErrNotFound", err)
	}
	if keys, err := store.List(t.Context(), "locked/"); err != nil || len(keys) != 0 {
		t.Fatalf("List after a failed Put = %v, %v; want nothing", keys, err)
	}
}

// The temporary-name prefix is reserved by the shared key rules, so every
// adapter rejects a key that uses it, not only the filesystem adapter whose
// List would hide it (CONTRACT_STORAGE §Put).
func TestStore_Put_RejectsReservedNames(t *testing.T) {
	stores := map[string]func(t *testing.T) Store{
		"fs": func(t *testing.T) Store {
			store, err := NewFS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"memory": func(*testing.T) Store { return NewMemory() },
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			for _, key := range []string{".lode-tmp-x", "a/.lode-tmp-cas-1"} {
				if err := store.Put(t.Context(), key, strings.NewReader("x")); !errors.Is(err, ErrInvalidPath) {
					t.Errorf("Put(%q) = %v, want ErrInvalidPath", key, err)
				}
			}
		})
	}
}

// A reader never sees a partially written object: while a Put is still
// streaming, Get reports ErrNotFound and List omits the object.
func TestFSStore_Put_PublishesAtomically(t *testing.T) {
	root := t.TempDir()
	store, err := NewFS(root)
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- store.Put(t.Context(), "obj", pr) }()
	if _, err := pw.Write([]byte("first half ")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(t.Context(), "obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get during a Put = %v, want ErrNotFound", err)
	}
	if keys, err := store.List(t.Context(), ""); err != nil || len(keys) != 0 {
		t.Fatalf("List during a Put = %v, %v; want nothing", keys, err)
	}
	_, _ = pw.Write([]byte("second half"))
	_ = pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	rc, err := store.Get(t.Context(), "obj")
	if err != nil {
		t.Fatal(err)
	}
	defer closer(rc)()
	if got, _ := io.ReadAll(rc); string(got) != "first half second half" {
		t.Fatalf("Get after the Put = %q", got)
	}
	assertSameMode(t, filepath.Join(root, "obj"), filepath.Join(t.TempDir(), "reference"))
}

// assertSameMode requires the object at path to have the mode that creating
// a file with mode 0644 gives under the process umask, as Put did before it
// published through a temporary file: the publication must not widen it.
func assertSameMode(t *testing.T, path, reference string) {
	t.Helper()
	f, err := os.OpenFile(reference, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(reference)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode().Perm() != want.Mode().Perm() {
		t.Fatalf("published object mode = %v, want %v", got.Mode().Perm(), want.Mode().Perm())
	}
}

// lockingReader chmods dir read-only on first Read, after Put has created
// the file inside it, then delegates to a failing reader.
type lockingReader struct {
	dir    string
	inner  io.Reader
	locked bool
}

func (l *lockingReader) Read(p []byte) (int, error) {
	if !l.locked {
		l.locked = true
		_ = os.Chmod(l.dir, 0o555)
	}
	return l.inner.Read(p)
}
