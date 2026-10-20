package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// A history is an append-only sequence of immutable snapshots with a head
// pointer. Dataset and Volume are both histories; they differ only in their
// path grammar and manifest type, which they supply as a snapshotKind.
//
// history owns everything about the head that the two containers used to
// duplicate: the persisted pointer, the in-process memory of the last head
// this writer observed (the CAS expected value for the next advance),
// self-heal after a scan, the scan itself, and manifest I/O.
//
// Single-writer per process: no mutex (CONTRACT_WRITE_API §Concurrency).

// snapshotKind is what a container knows about its own snapshots: where they
// live and how to read one. Implemented by small value types, not closures.
type snapshotKind[ID ~string, M any] interface {
	// snapshotsPrefix lists every manifest of the container.
	snapshotsPrefix() string
	// pointerPath is the head pointer object.
	pointerPath() string
	// manifestPath is the canonical manifest for a snapshot.
	manifestPath(id ID) string
	// parseManifest returns the snapshot ID a manifest path names, or false
	// for any other path.
	parseManifest(path string) (ID, bool)
	// decode reads and validates a manifest.
	decode(r io.Reader) (*M, error)
}

// snapshot pairs a snapshot ID with its decoded manifest.
type snapshot[ID ~string, M any] struct {
	ID       ID
	Manifest *M
}

type history[ID ~string, M any] struct {
	noun  string // what the kind calls its manifest in errors ("manifest", "volume manifest")
	store Store
	kind  snapshotKind[ID, M]

	// head is the snapshot this process last observed or committed as the
	// head ("" = unknown); pointer is the pointer content that corresponds to
	// it — what a CAS advance must expect. They differ only when a pointer
	// write failed after a commit.
	head    ID
	pointer string
}

// noun is what the kind calls its manifest in error messages, so a Volume
// failure still reads "volume manifest" after the protocol was shared.
func newHistory[ID ~string, M any](store Store, kind snapshotKind[ID, M], noun string) history[ID, M] {
	return history[ID, M]{noun: noun, store: store, kind: kind}
}

// -----------------------------------------------------------------------------
// Reading snapshots
// -----------------------------------------------------------------------------

// load decodes the manifest at path as snapshot id.
func (h *history[ID, M]) load(ctx context.Context, id ID, path string) (*snapshot[ID, M], error) {
	rc, err := h.store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	defer closer(rc)()
	m, err := h.kind.decode(rc)
	if err != nil {
		return nil, err
	}
	return &snapshot[ID, M]{ID: id, Manifest: m}, nil
}

// loadListed loads a manifest a listing returned. ok is false when it was
// deleted after the listing: an expiry removed it, which enumeration skips
// rather than reports (CONTRACT_RETENTION §Lineage).
func (h *history[ID, M]) loadListed(ctx context.Context, e manifestEntry[ID]) (s *snapshot[ID, M], ok bool, err error) {
	s, err = h.load(ctx, e.id, e.path)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return s, err == nil, err
}

// Snapshot loads a snapshot by ID from its canonical manifest.
func (h *history[ID, M]) Snapshot(ctx context.Context, id ID) (*snapshot[ID, M], error) {
	return h.load(ctx, id, h.kind.manifestPath(id))
}

// manifests lists every manifest path with the snapshot ID it names, one
// entry per snapshot, in no particular order. A snapshot may have several
// manifest copies (canonical plus one per partition); the lexicographically
// smallest path is kept so the same storage state always yields the same
// entries. One pass, no sort: a dataset prefix covers every data file too,
// and CONTRACT_COMPLEXITY bounds degraded Latest/Snapshot at O(N) in listed
// keys. Snapshots orders the entries itself, within its own S log S bound.
func (h *history[ID, M]) manifests(ctx context.Context) ([]manifestEntry[ID], error) {
	paths, err := h.store.List(ctx, h.kind.snapshotsPrefix())
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}
	byID := make(map[ID]string)
	for _, p := range paths {
		id, ok := h.kind.parseManifest(p)
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
func (h *history[ID, M]) Snapshots(ctx context.Context) ([]*snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b manifestEntry[ID]) int { return strings.Compare(string(a.id), string(b.id)) })
	out := make([]*snapshot[ID, M], 0, len(entries))
	for _, e := range entries {
		s, ok, err := h.loadListed(ctx, e)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to load snapshot %s: %w", e.id, err)
		}
		if ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// find locates a snapshot by scanning, for IDs whose canonical manifest is
// absent (pre-canonical layouts).
func (h *history[ID, M]) find(ctx context.Context, id ID) (*snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.id == id {
			return h.load(ctx, e.id, e.path)
		}
	}
	return nil, ErrNotFound
}

// scan finds the latest snapshot by listing: IDs are nanosecond timestamps,
// so the lexicographically largest ID is the latest. A manifest expired
// after the listing is passed over for the next largest. Returns
// ErrNoSnapshots when the container is empty.
func (h *history[ID, M]) scan(ctx context.Context) (*snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	// A max search per attempt, not a sort: CONTRACT_COMPLEXITY bounds the
	// degraded path at O(N), and a manifest vanishing mid-scan is rare.
	for len(entries) > 0 {
		i := latestEntry(entries)
		s, ok, err := h.loadListed(ctx, entries[i])
		if err != nil {
			return nil, fmt.Errorf("lode: failed to load latest snapshot: %w", err)
		}
		if ok {
			return s, nil
		}
		entries = slices.Delete(entries, i, i+1)
	}
	return nil, ErrNoSnapshots
}

// latestEntry is the index of the entry with the largest ID; entries must
// be non-empty.
func latestEntry[ID ~string](entries []manifestEntry[ID]) int {
	latest := 0
	for i := range entries {
		if entries[i].id > entries[latest].id {
			latest = i
		}
	}
	return latest
}

// -----------------------------------------------------------------------------
// The head pointer
// -----------------------------------------------------------------------------

// readPointer returns the ID the persisted pointer names, or ErrNotFound when
// the pointer is absent or empty.
func (h *history[ID, M]) readPointer(ctx context.Context) (ID, error) {
	rc, err := h.store.Get(ctx, h.kind.pointerPath())
	if err != nil {
		return "", err
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("lode: failed to read latest pointer: %w", err)
	}
	id := ID(strings.TrimSpace(string(data)))
	if id == "" {
		return "", ErrNotFound
	}
	return id, nil
}

// CommittedHead returns the head retention must never remove: the snapshot
// the pointer names, or, with no pointer, the one a scan returns ("" for an
// empty history). known is a head an earlier call confirmed; a pointer still
// naming it, or still absent, costs no further call. A pointer naming a
// snapshot without a canonical manifest is a commit in flight, whose parent
// the pointer no longer names: ErrHeadUncommitted.
func (h *history[ID, M]) CommittedHead(ctx context.Context, known ID) (ID, error) {
	id, err := h.readPointer(ctx)
	if errors.Is(err, ErrNotFound) {
		if known != "" {
			return known, nil
		}
		return h.scannedHead(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("lode: failed to resolve head: %w", err)
	}
	if id == known {
		return id, nil
	}
	exists, err := h.store.Exists(ctx, h.kind.manifestPath(id))
	if err != nil {
		return "", fmt.Errorf("lode: failed to resolve head: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("lode: latest pointer names %s: %w", id, ErrHeadUncommitted)
	}
	return id, nil
}

// scannedHead is the ID a scan would return, without loading its manifest.
func (h *history[ID, M]) scannedHead(ctx context.Context) (ID, error) {
	entries, err := h.manifests(ctx)
	if err != nil || len(entries) == 0 {
		return "", err
	}
	return entries[latestEntry(entries)].id, nil
}

// writePointer persists id as the pointer. With a ConditionalWriter the write
// is a CAS against expected (the pointer content the writer believes is
// current); otherwise Delete+Put under the single-writer assumption.
func (h *history[ID, M]) writePointer(ctx context.Context, expected string, id ID) error {
	path := h.kind.pointerPath()
	if cw, ok := h.store.(ConditionalWriter); ok {
		return cw.CompareAndSwap(ctx, path, expected, string(id))
	}
	_ = h.store.Delete(ctx, path) // path may not exist
	return h.store.Put(ctx, path, strings.NewReader(string(id)))
}

// remember records id as the observed head and what the pointer now holds.
func (h *history[ID, M]) remember(id ID, pointer string) {
	h.head, h.pointer = id, pointer
}

// Forget drops the in-process memory so the next Resolve consults storage.
func (h *history[ID, M]) Forget() {
	h.head, h.pointer = "", ""
}

// Latest returns the head snapshot: through the pointer when it names a
// loadable snapshot, otherwise by scan — and then self-heals the pointer (a
// CAS conflict there means a concurrent commit already advanced it). The
// result is remembered so a later advance expects the right pointer content.
func (h *history[ID, M]) Latest(ctx context.Context) (*snapshot[ID, M], error) {
	var observed string
	if id, err := h.readPointer(ctx); err == nil {
		if s, err := h.Snapshot(ctx, id); err == nil {
			h.remember(id, string(id))
			return s, nil
		}
		observed = string(id) // stale: names a missing snapshot
	}

	s, err := h.scan(ctx)
	if err != nil {
		return nil, err
	}
	// Self-heal is best-effort: a conflict means another writer repaired or
	// advanced the pointer first, and any other failure leaves the next
	// Latest to scan again. Either way the scan result stands.
	_ = h.writePointer(ctx, observed, s.ID)
	h.remember(s.ID, string(s.ID))
	return s, nil
}

// Resolve returns the ID a new commit parents on ("" for the first) and the
// pointer content a CAS advance must expect. Order: the in-process memory
// (authoritative within one writer), then the pointer when its manifest
// exists (one Exists call on cold start), then scan. A pointer naming a
// missing snapshot is stale: its content is still what CAS must expect, but
// the parent comes from the scan.
func (h *history[ID, M]) Resolve(ctx context.Context) (parent ID, expected string, err error) {
	id, _, expected, err := h.resolve(ctx, h.verifyExists)
	return id, expected, err
}

// ResolveParent is Resolve for containers whose commit needs the parent's
// manifest (a Volume merges its blocks): the parent is loaded, never
// re-fetched, and a remembered head whose manifest is gone is forgotten so
// storage is consulted instead. parent is nil for the first commit.
func (h *history[ID, M]) ResolveParent(ctx context.Context) (parent *snapshot[ID, M], expected string, err error) {
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
		if !errors.Is(err, ErrNotFound) {
			return nil, "", fmt.Errorf("lode: failed to load parent snapshot: %w", err)
		}
		h.Forget()
	}
}

// resolve walks memory → pointer → scan. verify decides whether the pointer's
// snapshot exists and may load it on the way; loaded is whatever verify or
// the scan produced (nil on the memory path, which never touches storage).
func (h *history[ID, M]) resolve(ctx context.Context, verify func(context.Context, ID) (*snapshot[ID, M], bool, error)) (id ID, loaded *snapshot[ID, M], expected string, err error) {
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
	if errors.Is(err, ErrNoSnapshots) {
		return "", nil, observed, nil
	}
	if err != nil {
		return "", nil, "", fmt.Errorf("lode: failed to get latest snapshot: %w", err)
	}
	return s.ID, s, observed, nil
}

// verifyExists confirms a snapshot by its canonical manifest's existence;
// a failed check counts as stale.
func (h *history[ID, M]) verifyExists(ctx context.Context, id ID) (*snapshot[ID, M], bool, error) { //nolint:unparam // shares the verifier signature with verifyLoad, which does return a snapshot
	exists, err := h.store.Exists(ctx, h.kind.manifestPath(id))
	return nil, err == nil && exists, nil
}

// verifyLoad confirms a snapshot by loading it; a missing manifest is stale,
// any other failure is an error.
func (h *history[ID, M]) verifyLoad(ctx context.Context, id ID) (*snapshot[ID, M], bool, error) {
	s, err := h.Snapshot(ctx, id)
	if err == nil {
		return s, true, nil
	}
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("lode: failed to load snapshot: %w", err)
}

// Advance commits a snapshot: the pointer first (CAS against expected), then
// every manifest path. The pointer goes first so a crash between the two
// leaves a pointer to a missing snapshot — which Resolve treats as stale and
// a cold start recovers by scan — never a committed manifest the pointer
// skips. On success the new head is remembered.
func (h *history[ID, M]) Advance(ctx context.Context, expected string, id ID, manifest *M, paths []string) error {
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
func (h *history[ID, M]) Refresh(ctx context.Context) error {
	if _, err := h.Latest(ctx); err != nil && !errors.Is(err, ErrNoSnapshots) {
		return err
	}
	return nil
}

// CommitLoop runs attempt until it succeeds or fails with anything other than
// ErrSnapshotConflict, backing off and refreshing the head between attempts.
// With maxAttempts 0 the attempt runs exactly once.
func (h *history[ID, M]) CommitLoop(ctx context.Context, retry *retryConfig, attempt func() error) error {
	var lastErr error
	for n := range retry.maxAttempts + 1 {
		if n > 0 {
			if err := retry.retryBackoff(ctx, n); err != nil {
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
		if !errors.Is(err, ErrSnapshotConflict) {
			return err
		}
		lastErr = err
	}
	return lastErr
}
