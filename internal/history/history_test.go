package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/retry"
)

// Unit tests for the history core in isolation: a map-backed store and a
// minimal kind, so every branch is reached without a container on top.
// Lode's Dataset and Volume tests exercise the same code end to end.
//
// Known equivalent mutant, left alive: smallestFirst's `<` → `<=` (two
// manifest copies of one snapshot are never the same path, so equality never
// occurs).

var (
	errNotFound = errors.New("not found")
	errNoSnaps  = errors.New("no snapshots")
	errConflict = errors.New("conflict")
	errBoom     = errors.New("boom")
	errHead     = errors.New("head uncommitted")
)

var testErrors = Errors{NotFound: errNotFound, NoSnapshots: errNoSnaps, SnapshotConflict: errConflict, HeadUncommitted: errHead}

// --- store ---

// memStore is a map-backed Store. fail injects an error per operation and
// path; reverse lists keys in descending order so first-seen is not
// smallest; ops records every call in order.
type memStore struct {
	objects map[string][]byte
	fail    func(op, path string) error
	reverse bool
	ops     []string
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (s *memStore) injected(op, path string) error {
	s.ops = append(s.ops, op+" "+path)
	if s.fail == nil {
		return nil
	}
	return s.fail(op, path)
}

func (s *memStore) Put(_ context.Context, path string, r io.Reader) error {
	if err := s.injected("put", path); err != nil {
		return err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.objects[path] = b
	return nil
}

func (s *memStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	if err := s.injected("get", path); err != nil {
		return nil, err
	}
	b, ok := s.objects[path]
	if !ok {
		return nil, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *memStore) Exists(_ context.Context, path string) (bool, error) {
	if err := s.injected("exists", path); err != nil {
		return false, err
	}
	_, ok := s.objects[path]
	return ok, nil
}

func (s *memStore) List(_ context.Context, prefix string) ([]string, error) {
	if err := s.injected("list", prefix); err != nil {
		return nil, err
	}
	var out []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	if s.reverse {
		slices.Reverse(out)
	}
	return out, nil
}

func (s *memStore) Delete(_ context.Context, path string) error {
	if err := s.injected("delete", path); err != nil {
		return err
	}
	delete(s.objects, path)
	return nil
}

// CompareAndSwap follows the ConditionalWriter table: expected "" means the
// object must be absent.
func (s *memStore) CompareAndSwap(_ context.Context, path, expected, replacement string) error {
	if err := s.injected("cas", path+" "+expected); err != nil {
		return err
	}
	cur, ok := s.objects[path]
	if (expected == "" && ok) || (expected != "" && string(cur) != expected) {
		return errConflict
	}
	s.objects[path] = []byte(replacement)
	return nil
}

// --- kind ---

type id string

type manifest struct {
	Tag string `json:"tag"`
}

// testKind lays snapshots out as c/snapshots/<id>/[copy/]manifest.json with
// the pointer at c/latest.
type testKind struct{}

const (
	prefix  = "c/snapshots/"
	pointer = "c/latest"
)

func (testKind) SnapshotsPrefix() string { return prefix }
func (testKind) ManifestPath(i id) string {
	return prefix + string(i) + "/manifest.json"
}

func (testKind) ParseManifest(p string) (id, bool) {
	rest, ok := strings.CutPrefix(p, prefix)
	if !ok || !strings.HasSuffix(p, "/manifest.json") {
		return "", false
	}
	seg, _, _ := strings.Cut(rest, "/")
	return id(seg), true
}

// NotExpired is always true: the core has no retention of its own.
func (testKind) NotExpired(context.Context, id) (bool, error) { return true, nil }

func (testKind) Decode(r io.Reader) (*manifest, error) {
	var m manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, errBoom
	}
	return &m, nil
}

func newTestHistory(s Store) History[id, manifest] {
	return New(s, testKind{}, pointer, "test manifest", testErrors)
}

// put stores a manifest with the given tag at the canonical path.
func put(t *testing.T, s *memStore, i id, tag string) {
	t.Helper()
	putAt(t, s, testKind{}.ManifestPath(i), tag)
}

func putAt(t *testing.T, s *memStore, path, tag string) {
	t.Helper()
	b, err := json.Marshal(manifest{Tag: tag})
	if err != nil {
		t.Fatal(err)
	}
	s.objects[path] = b
}

func failOn(op, pathPart string, err error) func(string, string) error {
	return func(o, p string) error {
		if o == op && strings.Contains(p, pathPart) {
			return err
		}
		return nil
	}
}

func wantMessage(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %q", err, want)
	}
}

// --- reading ---

func TestSnapshot(t *testing.T) {
	t.Run("loads the canonical manifest", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		h := newTestHistory(s)
		got, err := h.Snapshot(t.Context(), "1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "1" || got.Manifest.Tag != "one" {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("store error surfaces unwrapped", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		if _, err := h.Snapshot(t.Context(), "1"); !errors.Is(err, errNotFound) {
			t.Fatalf("got %v, want not found", err)
		}
	})
	t.Run("decode error surfaces", func(t *testing.T) {
		s := newMemStore()
		s.objects[testKind{}.ManifestPath("1")] = []byte("{")
		h := newTestHistory(s)
		if _, err := h.Snapshot(t.Context(), "1"); !errors.Is(err, errBoom) {
			t.Fatalf("got %v, want decode error", err)
		}
	})
}

func TestSnapshots(t *testing.T) {
	t.Run("ordered by ID, one per snapshot, non-manifests ignored", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "3", "three")
		put(t, s, "1", "one")
		put(t, s, "2", "two")
		putAt(t, s, prefix+"2/copy/manifest.json", "two")
		s.objects[prefix+"2/data/file"] = []byte("x")
		s.objects["c/elsewhere/manifest.json"] = []byte("{}")
		h := newTestHistory(s)
		got, err := h.Snapshots(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var ids []id
		for _, g := range got {
			ids = append(ids, g.ID)
		}
		if !slices.Equal(ids, []id{"1", "2", "3"}) {
			t.Fatalf("got %v, want [1 2 3]", ids)
		}
	})

	// With several copies of one snapshot, the smallest path is loaded no
	// matter the listing order.
	for _, reverse := range []bool{false, true} {
		t.Run("smallest copy wins, reverse="+strconv.FormatBool(reverse), func(t *testing.T) {
			s := newMemStore()
			s.reverse = reverse
			putAt(t, s, prefix+"1/a/manifest.json", "a")
			putAt(t, s, prefix+"1/b/manifest.json", "b")
			putAt(t, s, prefix+"1/manifest.json", "canonical")
			h := newTestHistory(s)
			got, err := h.Snapshots(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].Manifest.Tag != "a" {
				t.Fatalf("got %+v, want the copy at the smallest path", got)
			}
		})
	}

	t.Run("list error is wrapped", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("list", "", errBoom)
		h := newTestHistory(s)
		_, err := h.Snapshots(t.Context())
		wantMessage(t, err, "failed to list snapshots: boom")
	})

	t.Run("load error names the snapshot", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.fail = failOn("get", "", errBoom)
		h := newTestHistory(s)
		_, err := h.Snapshots(t.Context())
		wantMessage(t, err, "failed to load snapshot 1: boom")
	})
}

func TestFind(t *testing.T) {
	s := newMemStore()
	putAt(t, s, prefix+"7/copy/manifest.json", "seven")
	h := newTestHistory(s)

	got, err := h.Find(t.Context(), "7")
	if err != nil || got.Manifest.Tag != "seven" {
		t.Fatalf("Find(7): %+v, %v", got, err)
	}
	if _, err := h.Find(t.Context(), "8"); !errors.Is(err, errNotFound) {
		t.Fatalf("Find(8): got %v, want not found", err)
	}
	s.fail = failOn("list", "", errBoom)
	if _, err := h.Find(t.Context(), "7"); !errors.Is(err, errBoom) {
		t.Fatalf("Find with list error: got %v", err)
	}
}

// --- the head ---

func TestReadPointer(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    id
		wantErr error
	}{
		{"plain", "5", "5", nil},
		{"whitespace trimmed", " 5\n", "5", nil},
		{"empty is absent", "  \n", "", errNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newMemStore()
			s.objects[pointer] = []byte(c.content)
			h := newTestHistory(s)
			got, err := h.ReadPointer(t.Context())
			if got != c.want || !errors.Is(err, c.wantErr) {
				t.Fatalf("got %q, %v; want %q, %v", got, err, c.want, c.wantErr)
			}
		})
	}
	t.Run("read error is wrapped", func(t *testing.T) {
		h := newTestHistory(errReaderStore{newMemStore()})
		_, err := h.ReadPointer(t.Context())
		wantMessage(t, err, "lode: failed to read latest pointer: boom")
	})
}

// The removal marker reads as errs.Removed only for a container that passes
// that sentinel. A container that is never removed, which passes none, reads
// the marker as it reads any other pointer content.
func TestReadPointer_RemovalMarker(t *testing.T) {
	errRemoved := errors.New("removed")
	removable := testErrors
	removable.Removed = errRemoved

	s := newMemStore()
	s.objects[pointer] = []byte(RemovedPointer)
	h := New(s, testKind{}, pointer, "test manifest", removable)
	if _, err := h.ReadPointer(t.Context()); !errors.Is(err, errRemoved) {
		t.Fatalf("removable: got %v, want errs.Removed", err)
	}
	if _, err := h.Latest(t.Context()); !errors.Is(err, errRemoved) {
		t.Fatalf("Latest: got %v, want errs.Removed", err)
	}
	if _, _, err := h.Resolve(t.Context()); !errors.Is(err, errRemoved) {
		t.Fatalf("Resolve: got %v, want errs.Removed", err)
	}

	plain := newTestHistory(s)
	if got, err := plain.ReadPointer(t.Context()); err != nil || got != RemovedPointer {
		t.Fatalf("not removable: got %q, %v; want the marker as content", got, err)
	}
}

// Remove swaps whatever the pointer holds for the marker and returns what
// it replaced, creates the pointer when absent, and leaves an existing
// marker as it is.
func TestRemove(t *testing.T) {
	for _, content := range []string{"5", "", "not an id"} {
		s := newMemStore()
		if content != "" {
			s.objects[pointer] = []byte(content)
		}
		previous, err := Remove(t.Context(), s, pointer, testErrors, new(retry.Default()))
		if err != nil || previous != content {
			t.Fatalf("Remove over %q = %q, %v", content, previous, err)
		}
		if got := string(s.objects[pointer]); got != RemovedPointer {
			t.Fatalf("Remove over %q left %q", content, got)
		}
		previous, err = Remove(t.Context(), s, pointer, testErrors, new(retry.Default()))
		if err != nil || previous != RemovedPointer {
			t.Fatalf("second Remove over %q = %q, %v", content, previous, err)
		}
	}
	s := newMemStore()
	s.fail = failOn("get", "", errBoom)
	if _, err := Remove(t.Context(), s, pointer, testErrors, new(retry.Default())); !errors.Is(err, errBoom) {
		t.Fatalf("a failed pointer read: got %v, want it returned", err)
	}
}

// A commit that conflicts reads the pointer once more. The removal is
// returned when it finds the marker, and a read that fails otherwise is
// returned too, never hidden behind the conflict.
func TestCommitLoop_ReadsTheRemovalAfterAConflict(t *testing.T) {
	errRemoved := errors.New("removed")
	removable := testErrors
	removable.Removed = errRemoved
	conflict := func() error { return errConflict }

	s := newMemStore()
	s.objects[pointer] = []byte(RemovedPointer)
	h := New(s, testKind{}, pointer, "test manifest", removable)
	if err := h.CommitLoop(t.Context(), new(retry.Default()), conflict); !errors.Is(err, errRemoved) {
		t.Fatalf("a conflict over the marker: got %v, want errs.Removed", err)
	}

	s = newMemStore()
	s.objects[pointer] = []byte("5")
	s.fail = failOn("get", pointer, errBoom)
	h = New(s, testKind{}, pointer, "test manifest", removable)
	if err := h.CommitLoop(t.Context(), new(retry.Default()), conflict); !errors.Is(err, errBoom) {
		t.Fatalf("a conflict with a failing pointer read: got %v, want the read's error", err)
	}

	// A history that cannot be removed reads nothing after a conflict.
	h = New(s, testKind{}, pointer, "test manifest", testErrors)
	if err := h.CommitLoop(t.Context(), new(retry.Default()), conflict); !errors.Is(err, errConflict) {
		t.Fatalf("not removable: got %v, want the conflict", err)
	}
}

// errReaderStore returns a reader that fails, for ReadPointer's read error.
type errReaderStore struct{ *memStore }

func (errReaderStore) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(iotestErrReader{}), nil
}

type iotestErrReader struct{}

func (iotestErrReader) Read([]byte) (int, error) { return 0, errBoom }

func TestWritePointer(t *testing.T) {
	t.Run("CAS store swaps against expected", func(t *testing.T) {
		s := newMemStore()
		s.objects[pointer] = []byte("1")
		h := newTestHistory(s)
		if err := h.writePointer(t.Context(), "1", "2"); err != nil {
			t.Fatal(err)
		}
		if string(s.objects[pointer]) != "2" {
			t.Fatalf("pointer = %q, want 2", s.objects[pointer])
		}
		if err := h.writePointer(t.Context(), "1", "3"); !errors.Is(err, errConflict) {
			t.Fatalf("stale expected: got %v, want conflict", err)
		}
	})
}

// assertNoScan fails if the history listed its snapshots.
func assertNoScan(t *testing.T, s *memStore) {
	t.Helper()
	if slices.ContainsFunc(s.ops, func(op string) bool { return strings.HasPrefix(op, "list ") }) {
		t.Fatalf("scanned after a failed read: ops = %v", s.ops)
	}
}

// assertForgotten fails if the history still remembers a head, which a
// later Resolve would adopt without consulting storage.
func assertForgotten(t *testing.T, h History[id, manifest]) {
	t.Helper()
	if h.head != "" || h.pointer != "" {
		t.Fatalf("memory = %q/%q, want it discarded", h.head, h.pointer)
	}
}

func TestLatest(t *testing.T) {
	t.Run("pointer wins over a larger ID", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		put(t, s, "2", "two")
		s.objects[pointer] = []byte("1")
		h := newTestHistory(s)
		got, err := h.Latest(t.Context())
		if err != nil || got.ID != "1" {
			t.Fatalf("got %+v, %v; want snapshot 1", got, err)
		}
		if h.head != "1" || h.pointer != "1" {
			t.Fatalf("memory = %q/%q, want 1/1", h.head, h.pointer)
		}
	})
	t.Run("no pointer scans for the largest ID and heals", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		put(t, s, "3", "three")
		put(t, s, "2", "two")
		h := newTestHistory(s)
		got, err := h.Latest(t.Context())
		if err != nil || got.ID != "3" {
			t.Fatalf("got %+v, %v; want snapshot 3", got, err)
		}
		if string(s.objects[pointer]) != "3" || h.head != "3" || h.pointer != "3" {
			t.Fatalf("pointer = %q, memory = %q/%q", s.objects[pointer], h.head, h.pointer)
		}
	})
	// A pointer naming a snapshot with no manifest may be a commit in
	// flight; rewriting it would roll the head back over that commit.
	t.Run("stale pointer is left for the next commit", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("9")
		h := newTestHistory(s)
		h.remember("1", "1")
		got, err := h.Latest(t.Context())
		if err != nil || got.ID != "1" {
			t.Fatalf("got %+v, %v", got, err)
		}
		if slices.Contains(s.ops, "cas "+pointer+" 9") || string(s.objects[pointer]) != "9" {
			t.Fatalf("ops = %v, pointer = %q", s.ops, s.objects[pointer])
		}
		if h.head != "" || h.pointer != "" {
			t.Fatalf("memory = %q/%q, want it dropped", h.head, h.pointer)
		}
	})
	// A failed read is not evidence that the pointer or the manifest is
	// absent; a scan would return an older snapshot as the head (lode#208).
	t.Run("failed pointer read is returned, not scanned", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("1")
		s.fail = failOn("get", pointer, errBoom)
		h := newTestHistory(s)
		h.remember("1", "1")
		_, err := h.Latest(t.Context())
		wantMessage(t, err, "failed to read latest pointer: boom")
		assertNoScan(t, s)
		assertForgotten(t, h)
	})
	t.Run("failed head load is returned, not scanned", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		put(t, s, "2", "two")
		s.objects[pointer] = []byte("1")
		s.fail = failOn("get", testKind{}.ManifestPath("1"), errBoom)
		h := newTestHistory(s)
		h.remember("2", "2")
		_, err := h.Latest(t.Context())
		wantMessage(t, err, "failed to load latest snapshot: boom")
		assertNoScan(t, s)
		assertForgotten(t, h)
	})
	t.Run("failed creation still returns the scan result", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.fail = failOn("cas", "", errBoom)
		h := newTestHistory(s)
		got, err := h.Latest(t.Context())
		if err != nil || got.ID != "1" {
			t.Fatalf("got %+v, %v", got, err)
		}
		if h.head != "" || h.pointer != "" {
			t.Fatalf("memory = %q/%q, want none for a pointer not written", h.head, h.pointer)
		}
	})
	t.Run("empty history", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		if _, err := h.Latest(t.Context()); !errors.Is(err, errNoSnaps) {
			t.Fatalf("got %v, want no snapshots", err)
		}
	})
	t.Run("scan load error is wrapped", func(t *testing.T) {
		s := newMemStore()
		s.objects[testKind{}.ManifestPath("1")] = []byte("{")
		h := newTestHistory(s)
		_, err := h.Latest(t.Context())
		wantMessage(t, err, "failed to load latest snapshot: boom")
	})
	t.Run("scan list error surfaces", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("list", "", errBoom)
		h := newTestHistory(s)
		if _, err := h.Latest(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestResolve(t *testing.T) {
	t.Run("memory needs no storage", func(t *testing.T) {
		s := newMemStore()
		s.fail = func(string, string) error { return errBoom }
		h := newTestHistory(s)
		h.remember("4", "3")
		parent, expected, err := h.Resolve(t.Context())
		if parent != "4" || expected != "3" || err != nil || len(s.ops) != 0 {
			t.Fatalf("got %q/%q/%v, ops %v", parent, expected, err, s.ops)
		}
	})
	t.Run("pointer with a manifest", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		put(t, s, "2", "two")
		s.objects[pointer] = []byte("1")
		h := newTestHistory(s)
		parent, expected, err := h.Resolve(t.Context())
		if parent != "1" || expected != "1" || err != nil {
			t.Fatalf("got %q/%q/%v", parent, expected, err)
		}
	})
	// A pointer that names a snapshot without a manifest may belong to a
	// pending commit; adopting any other parent would fork the history.
	t.Run("stale pointer is the parent", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("9")
		h := newTestHistory(s)
		parent, expected, err := h.Resolve(t.Context())
		if parent != "9" || expected != "9" || err != nil {
			t.Fatalf("got %q/%q/%v", parent, expected, err)
		}
	})
	t.Run("pointer costs one Get and no manifest check", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("1")
		h := newTestHistory(s)
		s.ops = nil
		if _, _, err := h.Resolve(t.Context()); err != nil {
			t.Fatal(err)
		}
		if len(s.ops) != 1 {
			t.Fatalf("ops = %v, want only the pointer Get", s.ops)
		}
	})
	t.Run("pointer holding no snapshot ID: parent from scan, expected is the pointer", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("not-an-id")
		h := newTestHistory(s)
		parent, expected, err := h.Resolve(t.Context())
		if parent != "1" || expected != "not-an-id" || err != nil {
			t.Fatalf("got %q/%q/%v", parent, expected, err)
		}
	})
	t.Run("failed pointer read is returned, not scanned", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		put(t, s, "2", "two")
		s.objects[pointer] = []byte("1")
		s.fail = failOn("get", pointer, errBoom)
		h := newTestHistory(s)
		parent, expected, err := h.Resolve(t.Context())
		if parent != "" || expected != "" || !errors.Is(err, errBoom) {
			t.Fatalf("got %q/%q/%v", parent, expected, err)
		}
		assertNoScan(t, s)
	})
	t.Run("empty history parents on nothing", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		parent, expected, err := h.Resolve(t.Context())
		if parent != "" || expected != "" || err != nil {
			t.Fatalf("got %q/%q/%v", parent, expected, err)
		}
	})
	t.Run("scan error is wrapped", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("list", "", errBoom)
		h := newTestHistory(s)
		_, _, err := h.Resolve(t.Context())
		wantMessage(t, err, "failed to get latest snapshot: failed to list snapshots: boom")
	})
}

func TestResolveParent(t *testing.T) {
	t.Run("pointer path loads the parent once", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("1")
		h := newTestHistory(s)
		parent, expected, err := h.ResolveParent(t.Context())
		if err != nil || parent.ID != "1" || parent.Manifest.Tag != "one" || expected != "1" {
			t.Fatalf("got %+v/%q/%v", parent, expected, err)
		}
	})
	t.Run("memory path loads the remembered head", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "2", "two")
		h := newTestHistory(s)
		h.remember("2", "2")
		parent, expected, err := h.ResolveParent(t.Context())
		if err != nil || parent.ID != "2" || expected != "2" {
			t.Fatalf("got %+v/%q/%v", parent, expected, err)
		}
	})
	t.Run("remembered head gone: forget and use storage", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		h := newTestHistory(s)
		h.remember("9", "9")
		parent, expected, err := h.ResolveParent(t.Context())
		if err != nil || parent.ID != "1" || expected != "" {
			t.Fatalf("got %+v/%q/%v", parent, expected, err)
		}
	})
	t.Run("remembered head unreadable", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "2", "two")
		s.fail = failOn("get", "2/", errBoom)
		h := newTestHistory(s)
		h.remember("2", "2")
		_, _, err := h.ResolveParent(t.Context())
		wantMessage(t, err, "failed to load parent snapshot: boom")
	})
	t.Run("pointer's manifest unreadable", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("1")
		s.fail = failOn("get", "1/", errBoom)
		h := newTestHistory(s)
		_, _, err := h.ResolveParent(t.Context())
		wantMessage(t, err, "failed to load snapshot: boom")
	})
	// The blocks of a pending commit cannot be merged until its manifest is
	// persisted, so the commit conflicts and retries.
	t.Run("pointer's manifest missing is a conflict", func(t *testing.T) {
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("9")
		h := newTestHistory(s)
		parent, expected, err := h.ResolveParent(t.Context())
		if !errors.Is(err, errConflict) || parent != nil || expected != "" {
			t.Fatalf("got %+v/%q/%v, want a conflict", parent, expected, err)
		}
	})
	t.Run("first commit has no parent", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		parent, expected, err := h.ResolveParent(t.Context())
		if parent != nil || expected != "" || err != nil {
			t.Fatalf("got %+v/%q/%v", parent, expected, err)
		}
	})
}

func TestForget(t *testing.T) {
	h := newTestHistory(newMemStore())
	h.remember("1", "1")
	h.Forget()
	if h.head != "" || h.pointer != "" {
		t.Fatalf("memory = %q/%q, want empty", h.head, h.pointer)
	}
}

// --- committing ---

func TestAdvance(t *testing.T) {
	paths := []string{prefix + "5/manifest.json", prefix + "5/copy/manifest.json"}

	t.Run("pointer first, then every manifest, then remember", func(t *testing.T) {
		s := newMemStore()
		h := newTestHistory(s)
		if err := h.Advance(t.Context(), "", "5", &manifest{Tag: "five"}, paths); err != nil {
			t.Fatal(err)
		}
		want := []string{"cas " + pointer + " ", "put " + paths[0], "put " + paths[1]}
		if !slices.Equal(s.ops, want) {
			t.Fatalf("ops = %v, want %v", s.ops, want)
		}
		if h.head != "5" || h.pointer != "5" {
			t.Fatalf("memory = %q/%q", h.head, h.pointer)
		}
		got, err := h.Snapshot(t.Context(), "5")
		if err != nil || got.Manifest.Tag != "five" {
			t.Fatalf("written manifest: %+v, %v", got, err)
		}
	})
	t.Run("pointer failure is wrapped and nothing else is written", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("cas", pointer, errBoom)
		h := newTestHistory(s)
		err := h.Advance(t.Context(), "", "5", &manifest{}, paths)
		wantMessage(t, err, "failed to update latest pointer: boom")
		if errors.Is(err, ErrManifestUncertain) {
			t.Fatal("a pointer failure matches ErrManifestUncertain; no manifest was written")
		}
		if _, ok := s.objects[paths[0]]; ok || h.head != "" {
			t.Fatal("manifest written or head remembered after pointer failure")
		}
	})
	t.Run("manifest failure names the kind", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("put", "manifest.json", errBoom)
		h := newTestHistory(s)
		err := h.Advance(t.Context(), "", "5", &manifest{}, paths)
		wantMessage(t, err, "failed to write test manifest: boom")
		if !errors.Is(err, ErrManifestUncertain) || !errors.Is(err, errBoom) {
			t.Fatalf("manifest failure %v must match ErrManifestUncertain and its cause", err)
		}
		if h.head != "" {
			t.Fatal("head remembered after manifest failure")
		}
	})
	t.Run("marshal failure names the kind", func(t *testing.T) {
		h := New[id, unmarshalable](newMemStore(), unmarshalableKind{}, pointer, "test manifest", testErrors)
		err := h.Advance(t.Context(), "", "5", &unmarshalable{F: func() {}}, paths)
		if err == nil || !strings.HasPrefix(err.Error(), "lode: failed to marshal test manifest: ") {
			t.Fatalf("got %v", err)
		}
	})
}

// unmarshalable is a manifest type json cannot encode.
type unmarshalable struct{ F func() }

// unmarshalableKind is testKind for unmarshalable manifests.
type unmarshalableKind struct{ testKind }

func (unmarshalableKind) Decode(io.Reader) (*unmarshalable, error) { return nil, errBoom }

func TestRefresh(t *testing.T) {
	t.Run("empty is not an error", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		if err := h.Refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("store error surfaces", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("list", "", errBoom)
		h := newTestHistory(s)
		if err := h.Refresh(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
}

func fastRetry(t *testing.T, attempts int) *retry.Config {
	t.Helper()
	cfg := retry.Default()
	for _, err := range []error{cfg.SetMaxAttempts(attempts), cfg.SetBaseDelay(time.Nanosecond), cfg.SetMaxDelay(time.Nanosecond), cfg.SetJitter(0)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return &cfg
}

func TestCommitLoop(t *testing.T) {
	t.Run("no retries runs once", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		calls := 0
		err := h.CommitLoop(t.Context(), fastRetry(t, 0), func() error { calls++; return errConflict })
		if !errors.Is(err, errConflict) || calls != 1 {
			t.Fatalf("got %v after %d calls, want conflict after 1", err, calls)
		}
	})
	t.Run("persistent conflict runs attempts+1 times", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		calls := 0
		err := h.CommitLoop(t.Context(), fastRetry(t, 2), func() error { calls++; return errConflict })
		if !errors.Is(err, errConflict) || calls != 3 {
			t.Fatalf("got %v after %d calls, want conflict after 3", err, calls)
		}
	})
	t.Run("succeeds after a conflict, refreshing in between", func(t *testing.T) {
		s := newMemStore()
		h := newTestHistory(s)
		calls := 0
		err := h.CommitLoop(t.Context(), fastRetry(t, 2), func() error {
			calls++
			if calls == 1 {
				return errConflict
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("got %v after %d calls", err, calls)
		}
		if !slices.Contains(s.ops, "list "+prefix) {
			t.Fatalf("no refresh between attempts: ops %v", s.ops)
		}
	})
	t.Run("other errors are not retried", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		calls := 0
		err := h.CommitLoop(t.Context(), fastRetry(t, 3), func() error { calls++; return errBoom })
		if !errors.Is(err, errBoom) || calls != 1 {
			t.Fatalf("got %v after %d calls", err, calls)
		}
	})
	t.Run("canceled backoff stops the loop", func(t *testing.T) {
		h := newTestHistory(newMemStore())
		ctx, cancel := context.WithCancel(t.Context())
		cfg := retry.Default()
		if err := cfg.SetMaxAttempts(3); err != nil {
			t.Fatal(err)
		}
		calls := 0
		err := h.CommitLoop(ctx, &cfg, func() error { calls++; cancel(); return errConflict })
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("got %v after %d calls", err, calls)
		}
	})
	t.Run("refresh error stops the loop", func(t *testing.T) {
		s := newMemStore()
		h := newTestHistory(s)
		calls := 0
		err := h.CommitLoop(t.Context(), fastRetry(t, 3), func() error {
			calls++
			s.fail = failOn("list", "", errBoom)
			return errConflict
		})
		if !errors.Is(err, errBoom) || calls != 1 {
			t.Fatalf("got %v after %d calls", err, calls)
		}
	})
}

// --- staging ---

// stagingKind is testKind with staged manifests at c/staged/<id>.json,
// outside the snapshots prefix.
type stagingKind struct{ testKind }

func (stagingKind) StagedPath(i id) string   { return "c/staged/" + string(i) + ".json" }
func (stagingKind) StagesCommit(string) bool { return true }

// firstStagingKind stages only a commit that expects no pointer, as a
// dataset's first commit does.
type firstStagingKind struct{ stagingKind }

func (firstStagingKind) StagesCommit(expected string) bool { return expected == "" }

func newStagingHistory(s Store) History[id, manifest] {
	return New(s, stagingKind{}, pointer, "test manifest", testErrors)
}

func TestAdvance_Staged(t *testing.T) {
	paths := []string{prefix + "5/manifest.json"}
	staged := stagingKind{}.StagedPath("5")

	t.Run("staged copy precedes the pointer and is removed after the manifests", func(t *testing.T) {
		s := newMemStore()
		h := newStagingHistory(s)
		if err := h.Advance(t.Context(), "", "5", &manifest{Tag: "five"}, paths); err != nil {
			t.Fatal(err)
		}
		want := []string{"put " + staged, "cas " + pointer + " ", "put " + paths[0], "delete " + staged}
		if !slices.Equal(s.ops, want) {
			t.Fatalf("ops = %v, want %v", s.ops, want)
		}
		if _, ok := s.objects[staged]; ok {
			t.Fatal("staged copy remains after the manifests were persisted")
		}
	})
	t.Run("staging failure is wrapped and nothing else is written", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("put", staged, errBoom)
		h := newStagingHistory(s)
		err := h.Advance(t.Context(), "", "5", &manifest{}, paths)
		wantMessage(t, err, "failed to stage test manifest: boom")
		if len(s.ops) != 1 || h.head != "" {
			t.Fatalf("ops = %v, head = %q; want only the staging attempt", s.ops, h.head)
		}
	})
	t.Run("manifest failure retains the staged copy", func(t *testing.T) {
		s := newMemStore()
		s.fail = failOn("put", "manifest.json", errBoom)
		h := newStagingHistory(s)
		if err := h.Advance(t.Context(), "", "5", &manifest{Tag: "five"}, paths); !errors.Is(err, ErrManifestUncertain) {
			t.Fatalf("got %v, want ErrManifestUncertain", err)
		}
		if _, ok := s.objects[staged]; !ok {
			t.Fatal("staged copy removed although the canonical manifest is absent")
		}
	})
	t.Run("lost race discards the staged copy", func(t *testing.T) {
		s := newMemStore()
		s.objects[pointer] = []byte("4")
		h := newStagingHistory(s)
		if err := h.Advance(t.Context(), "", "5", &manifest{}, paths); !errors.Is(err, errConflict) {
			t.Fatalf("got %v, want a conflict", err)
		}
		if _, ok := s.objects[staged]; ok {
			t.Fatal("staged copy of a commit the pointer does not name remains")
		}
	})
	// A store may report a failure for a CAS it applied (a retry after a lost
	// response); the staged copy is then the only manifest of a committed
	// snapshot.
	t.Run("pointer failure the store applied retains the staged copy", func(t *testing.T) {
		s := newMemStore()
		s.fail = func(op, _ string) error {
			if op == "cas" {
				s.objects[pointer] = []byte("5")
				return errBoom
			}
			return nil
		}
		h := newStagingHistory(s)
		if err := h.Advance(t.Context(), "", "5", &manifest{}, paths); !errors.Is(err, errBoom) {
			t.Fatalf("got %v, want boom", err)
		}
		if _, ok := s.objects[staged]; !ok {
			t.Fatal("staged copy of the snapshot the pointer names was removed")
		}
	})
}

func TestAdvance_StagesOnlyAcceptedCommits(t *testing.T) {
	for _, tc := range []struct {
		name, expected string
		staged         bool
	}{
		{"a commit that expects no pointer stages", "", true},
		{"a commit that expects a pointer does not", "4", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemStore()
			if tc.expected != "" {
				s.objects[pointer] = []byte(tc.expected)
			}
			h := New(s, firstStagingKind{}, pointer, "test manifest", testErrors)
			if err := h.Advance(t.Context(), tc.expected, "5", &manifest{}, []string{prefix + "5/manifest.json"}); err != nil {
				t.Fatal(err)
			}
			staged := slices.Contains(s.ops, "put "+firstStagingKind{}.StagedPath("5"))
			if staged != tc.staged {
				t.Fatalf("staged = %v, want %v; ops = %v", staged, tc.staged, s.ops)
			}
		})
	}
}

func TestLoadStaged(t *testing.T) {
	s := newMemStore()
	putAt(t, s, stagingKind{}.StagedPath("9"), "nine")
	h := newStagingHistory(s)
	got, err := h.LoadStaged(t.Context(), "9")
	if err != nil || got.Manifest.Tag != "nine" {
		t.Fatalf("got %+v/%v, want the staged copy", got, err)
	}
	plain := New(s, testKind{}, pointer, "test manifest", testErrors)
	if _, err := plain.LoadStaged(t.Context(), "9"); !errors.Is(err, testErrors.NotFound) {
		t.Fatalf("got %v, want NotFound for a kind that stages nothing", err)
	}
}

func TestResolveParent_Staged(t *testing.T) {
	staged := stagingKind{}.StagedPath("9")
	pending := func(t *testing.T) *memStore {
		t.Helper()
		s := newMemStore()
		put(t, s, "1", "one")
		s.objects[pointer] = []byte("9")
		return s
	}
	t.Run("an abandoned commit's staged copy is the parent", func(t *testing.T) {
		s := pending(t)
		putAt(t, s, staged, "nine")
		h := newStagingHistory(s)
		parent, expected, err := h.ResolveParent(t.Context())
		if err != nil || parent.ID != "9" || parent.Manifest.Tag != "nine" || expected != "9" {
			t.Fatalf("got %+v/%q/%v, want 9 from its staged copy", parent, expected, err)
		}
	})
	// The writer persisted the canonical manifest and removed the staged copy
	// between the two reads.
	t.Run("a staged copy removed meanwhile falls back to the canonical manifest", func(t *testing.T) {
		s := pending(t)
		s.fail = func(op, path string) error {
			if op == "get" && path == staged {
				putAt(t, s, testKind{}.ManifestPath("9"), "nine")
			}
			return nil
		}
		h := newStagingHistory(s)
		parent, _, err := h.ResolveParent(t.Context())
		if err != nil || parent.Manifest.Tag != "nine" {
			t.Fatalf("got %+v/%v, want 9 from its canonical manifest", parent, err)
		}
	})
	t.Run("neither copy is a conflict", func(t *testing.T) {
		h := newStagingHistory(pending(t))
		parent, expected, err := h.ResolveParent(t.Context())
		if !errors.Is(err, errConflict) || parent != nil || expected != "" {
			t.Fatalf("got %+v/%q/%v, want a conflict", parent, expected, err)
		}
	})
	t.Run("an unreadable staged copy is returned", func(t *testing.T) {
		s := pending(t)
		s.fail = failOn("get", staged, errBoom)
		h := newStagingHistory(s)
		_, _, err := h.ResolveParent(t.Context())
		wantMessage(t, err, "failed to load staged snapshot: boom")
	})
	t.Run("an unreadable canonical manifest after a removed staged copy is returned", func(t *testing.T) {
		s := pending(t)
		canonical := testKind{}.ManifestPath("9")
		gets := 0
		s.fail = func(op, path string) error {
			if op == "get" && path == canonical {
				if gets++; gets > 1 {
					return errBoom
				}
			}
			return nil
		}
		h := newStagingHistory(s)
		_, _, err := h.ResolveParent(t.Context())
		wantMessage(t, err, "failed to load snapshot: boom")
	})
}
