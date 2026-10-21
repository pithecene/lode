// Package history is the snapshot-history core shared by Lode's containers.
//
// A History is an append-only sequence of immutable snapshots with a head
// pointer. Dataset and Volume are both histories; they differ only in their
// path grammar and manifest type, which they supply as a Kind.
//
// History owns everything about the head that the containers would otherwise
// duplicate: the persisted pointer, the in-process memory of the last head
// this writer observed (the CAS expected value for the next advance), the
// scan, and manifest I/O. Only a commit writes the pointer: a read never does.
//
// The package does not import lode: it reads the store through the Store
// subset below and returns the sentinels the container passes in (Errors),
// so callers still see lode's error identities.
//
// Single-writer per process: no mutex (CONTRACT_WRITE_API §Concurrency).
package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/pithecene-io/lode/internal/retry"
)

// Store is the subset of lode.Store a history uses. Any lode.Store satisfies
// it.
type Store interface {
	Put(ctx context.Context, path string, r io.Reader) error
	Get(ctx context.Context, path string) (io.ReadCloser, error)
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, path string) error
}

// conditionalWriter mirrors lode.ConditionalWriter: a store that implements
// it gets CAS pointer updates.
type conditionalWriter interface {
	CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

// Errors are the sentinels a history returns and matches. The container
// passes lode's own (ErrNotFound, ErrNoSnapshots, ErrSnapshotConflict), so
// errors keep their public identity.
type Errors struct {
	NotFound         error
	NoSnapshots      error
	SnapshotConflict error
}

// Kind is what a container knows about its own snapshots: where they live
// and how to read one. Implemented by small value types, not closures.
type Kind[ID ~string, M any] interface {
	// SnapshotsPrefix lists every manifest of the container.
	SnapshotsPrefix() string
	// PointerPath is the head pointer object.
	PointerPath() string
	// ManifestPath is the canonical manifest for a snapshot.
	ManifestPath(id ID) string
	// ParseManifest returns the snapshot ID a manifest path names, or false
	// for any other path.
	ParseManifest(path string) (ID, bool)
	// Decode reads and validates a manifest.
	Decode(r io.Reader) (*M, error)
}

// Snapshot pairs a snapshot ID with its decoded manifest.
type Snapshot[ID ~string, M any] struct {
	ID       ID
	Manifest *M
}

// NewID returns a new snapshot ID: the current time in Unix nanoseconds.
// Scan relies on IDs ordering by creation time.
func NewID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// History is one container's snapshot sequence and head pointer.
type History[ID ~string, M any] struct {
	noun  string // what the kind calls its manifest in errors ("manifest", "volume manifest")
	store Store
	kind  Kind[ID, M]
	errs  Errors

	// head is the snapshot this process last observed or committed as the
	// head ("" = unknown); pointer is the pointer content that corresponds to
	// it — what a CAS advance must expect. They differ only when a pointer
	// write failed after a commit.
	head    ID
	pointer string
}

// New returns an empty-memory history over store. noun is what the kind
// calls its manifest in error messages, so a Volume failure still reads
// "volume manifest" after the protocol was shared.
func New[ID ~string, M any](store Store, kind Kind[ID, M], noun string, errs Errors) History[ID, M] {
	return History[ID, M]{noun: noun, store: store, kind: kind, errs: errs}
}

// -----------------------------------------------------------------------------
// Reading snapshots
// -----------------------------------------------------------------------------

// load decodes the manifest at path as snapshot id.
func (h *History[ID, M]) load(ctx context.Context, id ID, path string) (*Snapshot[ID, M], error) {
	rc, err := h.store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }() // read-only stream
	m, err := h.kind.Decode(rc)
	if err != nil {
		return nil, err
	}
	return &Snapshot[ID, M]{ID: id, Manifest: m}, nil
}

// Snapshot loads a snapshot by ID from its canonical manifest.
func (h *History[ID, M]) Snapshot(ctx context.Context, id ID) (*Snapshot[ID, M], error) {
	return h.load(ctx, id, h.kind.ManifestPath(id))
}

// manifests lists every manifest path with the snapshot ID it names, one
// entry per snapshot, in no particular order. A snapshot may have several
// manifest copies (canonical plus one per partition); the lexicographically
// smallest path is kept so the same storage state always yields the same
// entries. One pass, no sort: a dataset prefix covers every data file too,
// and CONTRACT_COMPLEXITY bounds degraded Latest/Snapshot at O(N) in listed
// keys. Snapshots orders the entries itself, within its own S log S bound.
func (h *History[ID, M]) manifests(ctx context.Context) ([]manifestEntry[ID], error) {
	paths, err := h.store.List(ctx, h.kind.SnapshotsPrefix())
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}
	byID := make(map[ID]string)
	for _, p := range paths {
		id, ok := h.kind.ParseManifest(p)
		if !ok {
			continue
		}
		if cur, seen := byID[id]; !seen || p < cur {
			byID[id] = p
		}
	}
	out := make([]manifestEntry[ID], 0, len(byID))
	for id, p := range byID {
		out = append(out, manifestEntry[ID]{id, p})
	}
	return out, nil
}

type manifestEntry[ID ~string] struct {
	id   ID
	path string
}

// Snapshots loads every snapshot, ordered by ID so that callers sorting on
// a manifest field get a deterministic tie-break.
func (h *History[ID, M]) Snapshots(ctx context.Context) ([]*Snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b manifestEntry[ID]) int { return strings.Compare(string(a.id), string(b.id)) })
	out := make([]*Snapshot[ID, M], 0, len(entries))
	for _, e := range entries {
		s, err := h.load(ctx, e.id, e.path)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to load snapshot %s: %w", e.id, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// Find locates a snapshot by scanning, for IDs whose canonical manifest is
// absent (pre-canonical layouts).
func (h *History[ID, M]) Find(ctx context.Context, id ID) (*Snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.id == id {
			return h.load(ctx, e.id, e.path)
		}
	}
	return nil, h.errs.NotFound
}

// scan finds the latest snapshot by listing: IDs are nanosecond timestamps,
// so the lexicographically largest ID is the latest. Returns errs.NoSnapshots
// when the container is empty.
func (h *History[ID, M]) scan(ctx context.Context) (*Snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	var latest *manifestEntry[ID]
	for i := range entries {
		if latest == nil || entries[i].id > latest.id {
			latest = &entries[i]
		}
	}
	if latest == nil {
		return nil, h.errs.NoSnapshots
	}
	s, err := h.load(ctx, latest.id, latest.path)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to load latest snapshot: %w", err)
	}
	return s, nil
}

// -----------------------------------------------------------------------------
// The head pointer
// -----------------------------------------------------------------------------

// readPointer returns the ID the persisted pointer names, or errs.NotFound when
// the pointer is absent or empty.
func (h *History[ID, M]) readPointer(ctx context.Context) (ID, error) {
	rc, err := h.store.Get(ctx, h.kind.PointerPath())
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }() // read-only stream
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("lode: failed to read latest pointer: %w", err)
	}
	id := ID(strings.TrimSpace(string(data)))
	if id == "" {
		return "", h.errs.NotFound
	}
	return id, nil
}

// writePointer persists id as the pointer. With a ConditionalWriter the write
// is a CAS against expected (the pointer content the writer believes is
// current); otherwise Delete+Put under the single-writer assumption.
func (h *History[ID, M]) writePointer(ctx context.Context, expected string, id ID) error {
	path := h.kind.PointerPath()
	if cw, ok := h.store.(conditionalWriter); ok {
		return cw.CompareAndSwap(ctx, path, expected, string(id))
	}
	_ = h.store.Delete(ctx, path) // path may not exist
	return h.store.Put(ctx, path, strings.NewReader(string(id)))
}

// remember records id as the observed head and what the pointer now holds.
func (h *History[ID, M]) remember(id ID, pointer string) {
	h.head, h.pointer = id, pointer
}

// Forget drops the in-process memory so the next Resolve consults storage.
func (h *History[ID, M]) Forget() {
	h.head, h.pointer = "", ""
}

// Latest returns the head snapshot: through the pointer when it names a
// loadable snapshot, otherwise by scan. A pointer or manifest read that fails
// for any reason other than absence is returned, never read as "stale": a
// transient error on a valid head must not substitute an older snapshot
// (lode#208). Latest never writes the pointer — a scan result is not proof of
// the head, and writing it back could rewind the pointer past a commit. Only
// a pointer-confirmed head is remembered as the CAS expectation.
func (h *History[ID, M]) Latest(ctx context.Context) (*Snapshot[ID, M], error) {
	id, err := h.readPointer(ctx)
	if err == nil {
		s, err := h.Snapshot(ctx, id)
		if err == nil {
			h.remember(id, string(id))
			return s, nil
		}
		if !errors.Is(err, h.errs.NotFound) {
			return nil, fmt.Errorf("lode: failed to load latest snapshot: %w", err)
		}
	} else if !errors.Is(err, h.errs.NotFound) {
		return nil, fmt.Errorf("lode: failed to read latest pointer: %w", err)
	}

	s, err := h.scan(ctx)
	if err != nil {
		return nil, err
	}
	h.Forget()
	return s, nil
}

// Resolve returns the ID a new commit parents on ("" for the first) and the
// pointer content a CAS advance must expect. Order: the in-process memory
// (authoritative within one writer), then the pointer when its manifest
// exists (one Exists call on cold start), then scan. A pointer naming a
// missing snapshot is stale: its content is still what CAS must expect, but
// the parent comes from the scan.
func (h *History[ID, M]) Resolve(ctx context.Context) (parent ID, expected string, err error) {
	id, _, expected, err := h.resolve(ctx, h.verifyExists)
	return id, expected, err
}

// ResolveParent is Resolve for containers whose commit needs the parent's
// manifest (a Volume merges its blocks): the parent is loaded, never
// re-fetched, and a remembered head whose manifest is gone is forgotten so
// storage is consulted instead. parent is nil for the first commit.
func (h *History[ID, M]) ResolveParent(ctx context.Context) (parent *Snapshot[ID, M], expected string, err error) {
	for {
		id, loaded, expected, err := h.resolve(ctx, h.verifyLoad)
		if err != nil || id == "" || loaded != nil {
			return loaded, expected, err
		}
		// The head came from memory; load it, dropping the memory if stale.
		loaded, err = h.Snapshot(ctx, id)
		if err == nil {
			return loaded, expected, nil
		}
		if !errors.Is(err, h.errs.NotFound) {
			return nil, "", fmt.Errorf("lode: failed to load parent snapshot: %w", err)
		}
		h.Forget()
	}
}

// resolve walks memory → pointer → scan. verify decides whether the pointer's
// snapshot exists and may load it on the way; loaded is whatever verify or
// the scan produced (nil on the memory path, which never touches storage).
func (h *History[ID, M]) resolve(ctx context.Context, verify func(context.Context, ID) (*Snapshot[ID, M], bool, error)) (id ID, loaded *Snapshot[ID, M], expected string, err error) {
	if h.head != "" {
		return h.head, nil, h.pointer, nil
	}

	var observed string
	if id, err := h.readPointer(ctx); err == nil {
		s, ok, err := verify(ctx, id)
		if err != nil {
			return "", nil, "", err
		}
		if ok {
			return id, s, string(id), nil
		}
		observed = string(id)
	}

	s, err := h.scan(ctx)
	if errors.Is(err, h.errs.NoSnapshots) {
		return "", nil, observed, nil
	}
	if err != nil {
		return "", nil, "", fmt.Errorf("lode: failed to get latest snapshot: %w", err)
	}
	return s.ID, s, observed, nil
}

// verifyExists confirms a snapshot by its canonical manifest's existence; a
// missing manifest is stale, a failed check is an error (lode#208: treating
// it as stale parented a commit on an older snapshot).
func (h *History[ID, M]) verifyExists(ctx context.Context, id ID) (*Snapshot[ID, M], bool, error) { //nolint:unparam // shares the verifier signature with verifyLoad, which does return a snapshot
	exists, err := h.store.Exists(ctx, h.kind.ManifestPath(id))
	if err != nil {
		return nil, false, fmt.Errorf("lode: failed to check snapshot: %w", err)
	}
	return nil, exists, nil
}

// verifyLoad confirms a snapshot by loading it; a missing manifest is stale,
// any other failure is an error.
func (h *History[ID, M]) verifyLoad(ctx context.Context, id ID) (*Snapshot[ID, M], bool, error) {
	s, err := h.Snapshot(ctx, id)
	if err == nil {
		return s, true, nil
	}
	if errors.Is(err, h.errs.NotFound) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("lode: failed to load snapshot: %w", err)
}

// Advance commits a snapshot: the pointer first (CAS against expected), then
// every manifest path. The pointer goes first so a crash between the two
// leaves a pointer to a missing snapshot — which Resolve treats as stale and
// a cold start recovers by scan — never a committed manifest the pointer
// skips. On success the new head is remembered.
func (h *History[ID, M]) Advance(ctx context.Context, expected string, id ID, manifest *M, paths []string) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("lode: failed to marshal %s: %w", h.noun, err)
	}
	if err := h.writePointer(ctx, expected, id); err != nil {
		return fmt.Errorf("lode: failed to update latest pointer: %w", err)
	}
	for _, p := range paths {
		if err := h.store.Put(ctx, p, bytes.NewReader(data)); err != nil {
			return fmt.Errorf("lode: failed to write %s: %w", h.noun, err)
		}
	}
	h.remember(id, string(id))
	return nil
}

// Refresh re-reads the head from storage between commit attempts so a retry
// parents on what a concurrent writer committed. An empty history is not an
// error here.
func (h *History[ID, M]) Refresh(ctx context.Context) error {
	if _, err := h.Latest(ctx); err != nil && !errors.Is(err, h.errs.NoSnapshots) {
		return err
	}
	return nil
}

// CommitLoop runs attempt until it succeeds or fails with anything other than
// errs.SnapshotConflict, backing off and refreshing the head between attempts.
// With maxAttempts 0 the attempt runs exactly once.
func (h *History[ID, M]) CommitLoop(ctx context.Context, cfg *retry.Config, attempt func() error) error {
	var lastErr error
	for n := range cfg.MaxAttempts() + 1 {
		if n > 0 {
			if err := cfg.Backoff(ctx, n); err != nil {
				return err
			}
			if err := h.Refresh(ctx); err != nil {
				return err
			}
		}
		err := attempt()
		if err == nil {
			return nil
		}
		if !errors.Is(err, h.errs.SnapshotConflict) {
			return err
		}
		lastErr = err
	}
	return lastErr
}
