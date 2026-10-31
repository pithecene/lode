// Package history is the snapshot-history core shared by Lode's containers.
//
// A History is an append-only sequence of immutable snapshots with a head
// pointer. Dataset and Volume are both histories; they differ only in their
// path grammar and manifest type, which they supply as a Kind.
//
// History owns everything about the head that the containers would otherwise
// duplicate: the persisted pointer, the in-process memory of the last head
// this writer observed (the CAS expected value for the next advance),
// creating a missing pointer after a scan, the scan itself, and manifest I/O.
//
// The package does not import lode: it reads the store through the Store
// subset below and returns the sentinels the container passes in (Errors),
// so callers still see lode's error identities.
//
// Single-writer per process: no mutex (CONTRACT_WRITE_API §Concurrency).
package history

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/retry"
)

// Store is the subset of lode.Store a history uses. Any lode.Store satisfies
// it.
type Store interface {
	Put(ctx context.Context, path string, r io.Reader) error
	Get(ctx context.Context, path string) (io.ReadCloser, error)
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, path string) error
	CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

// Errors are the sentinels a history returns and matches. The container
// passes lode's own (ErrNotFound, ErrNoSnapshots, ErrSnapshotConflict), so
// errors keep their public identity.
type Errors struct {
	NotFound         error
	NoSnapshots      error
	SnapshotConflict error
	// HeadUncommitted is returned by CommittedHead when the pointer names a
	// snapshot with no manifest, or is missing beside existing snapshots.
	HeadUncommitted error
	// Removed is returned wherever the head is read when the pointer holds
	// RemovedPointer. A container that is never removed leaves it nil, and
	// its pointer is then read as before.
	Removed error
}

// RemovedPointer is the content of the head pointer of a removed history
// (CONTRACT_CORE §Head Pointer). It is not a snapshot ID, so no commit can
// parent on it, and Remove never deletes it, so a writer that held the
// history open through the removal can never advance the pointer again.
const RemovedPointer = "lode:removed"

// Kind is what a container knows about its own snapshots: where they live
// and how to read one. Implemented by small value types, not closures.
type Kind[ID ~string, M any] interface {
	// SnapshotsPrefix lists every manifest of the container.
	SnapshotsPrefix() string
	// ManifestPath is the canonical manifest for a snapshot.
	ManifestPath(id ID) string
	// ParseManifest returns the snapshot ID a manifest path names, or false
	// for any other path.
	ParseManifest(path string) (ID, bool)
	// Decode reads and validates a manifest.
	Decode(r io.Reader) (*M, error)
	// NotExpired reports whether a pointer-less scan may take snapshot id as
	// the head: false only for one retention has expired.
	NotExpired(ctx context.Context, id ID) (bool, error)
}

// Stager is implemented by a Kind whose commits must be loadable while
// pending: a Volume merges its parent's blocks, and a dataset's writer
// checks its settings against the head's manifest. For each commit that
// StagesCommit accepts, Advance persists a staged copy of the manifest at
// StagedPath before it advances the pointer, so the next commit can load the
// parent even when its writer terminated before persisting the canonical
// manifest (CONTRACT_CORE §Head Pointer). StagedPath MUST NOT parse as a
// manifest of the kind: a staged copy is never listed as a snapshot.
type Stager[ID ~string] interface {
	StagedPath(id ID) string
	// StagesCommit reports whether a commit that expects the given pointer
	// content stages its manifest.
	StagesCommit(expected string) bool
}

// Snapshot pairs a snapshot ID with its decoded manifest.
type Snapshot[ID ~string, M any] struct {
	ID       ID
	Manifest *M
}

// History is one container's snapshot sequence and head pointer.
type History[ID ~string, M any] struct {
	noun  string // what the kind calls its manifest in errors ("manifest", "volume manifest")
	store Store
	kind  Kind[ID, M]
	errs  Errors
	// stager is the kind's Stager, nil when its commits stage nothing.
	stager Stager[ID]
	// pointerPath is the head pointer object, fixed at construction.
	pointerPath string

	// head is the snapshot this process last observed or committed as the
	// head ("" = unknown); pointer is the pointer content that corresponds to
	// it — what a CAS advance must expect. They differ only when a pointer
	// write failed after a commit.
	head    ID
	pointer string
}

// New returns an empty-memory history over store whose head pointer is the
// object at pointerPath. The path is a value, not a Kind method, so it
// cannot change during the life of the history. noun is what the kind
// calls its manifest in error messages, so a Volume failure still reads
// "volume manifest" after the protocol was shared.
func New[ID ~string, M any](store Store, kind Kind[ID, M], pointerPath, noun string, errs Errors) History[ID, M] {
	stager, _ := kind.(Stager[ID])
	return History[ID, M]{
		noun: noun, store: store, kind: kind, errs: errs,
		stager: stager, pointerPath: pointerPath,
	}
}

// -----------------------------------------------------------------------------
// Reading snapshots
// -----------------------------------------------------------------------------

// LoadAt decodes the manifest copy at path as snapshot id.
func (h *History[ID, M]) LoadAt(ctx context.Context, id ID, path string) (*Snapshot[ID, M], error) {
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
	return h.LoadAt(ctx, id, h.kind.ManifestPath(id))
}

// manifests lists every manifest path with the snapshot ID it names, one
// entry per snapshot, in no particular order. A snapshot may have several
// manifest copies (canonical plus one per partition); all are kept, the
// lexicographically smallest first, so the same storage state always loads
// the same copy and a copy deleted after the listing falls back to another.
// One pass, no sort: a dataset prefix covers every data file too,
// and CONTRACT_COMPLEXITY bounds degraded Latest/Snapshot at O(N) in listed
// keys. Snapshots orders the entries itself, within its own S log S bound.
func (h *History[ID, M]) manifests(ctx context.Context) ([]manifestEntry[ID], error) {
	paths, err := h.store.List(ctx, h.kind.SnapshotsPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	byID := make(map[ID][]string)
	for _, p := range paths {
		if id, ok := h.kind.ParseManifest(p); ok {
			byID[id] = smallestFirst(append(byID[id], p))
		}
	}
	out := make([]manifestEntry[ID], 0, len(byID))
	for id, copies := range byID {
		out = append(out, manifestEntry[ID]{id, copies})
	}
	return out, nil
}

// smallestFirst moves the last path to the front when it is the smallest,
// so the front is always the minimum without sorting the rest.
func smallestFirst(paths []string) []string {
	if last := len(paths) - 1; paths[last] < paths[0] {
		paths[0], paths[last] = paths[last], paths[0]
	}
	return paths
}

// manifestEntry is one snapshot a listing found, with every manifest copy
// it showed, smallest path first.
type manifestEntry[ID ~string] struct {
	id    ID
	paths []string
}

// loadEntry loads the snapshot from the first of its listed copies that is
// still present. errs.NotFound means an expiry removed every copy after the
// listing, which enumeration skips rather than reports
// (CONTRACT_RETENTION §Lineage).
func (h *History[ID, M]) loadEntry(ctx context.Context, e manifestEntry[ID]) (*Snapshot[ID, M], error) {
	for _, p := range e.paths {
		s, err := h.LoadAt(ctx, e.id, p)
		if !errors.Is(err, h.errs.NotFound) {
			return s, err
		}
	}
	return nil, fmt.Errorf("lode: snapshot %s: every listed manifest is gone: %w", e.id, h.errs.NotFound)
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
		// A manifest an expiry deleted after the listing is skipped, not
		// reported (CONTRACT_RETENTION §Lineage).
		s, err := h.loadEntry(ctx, e)
		if errors.Is(err, h.errs.NotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to load snapshot %s: %w", e.id, err)
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
			return h.loadEntry(ctx, e)
		}
	}
	return nil, h.errs.NotFound
}

// scan finds the latest snapshot by listing: IDs begin with a nanosecond
// instant, so the lexicographically largest ID is the latest. A manifest
// expired after the listing is passed over for the next largest. Returns
// errs.NoSnapshots when the container is empty.
func (h *History[ID, M]) scan(ctx context.Context) (*Snapshot[ID, M], error) {
	entries, err := h.manifests(ctx)
	if err != nil {
		return nil, err
	}
	// A max-heap, not a sort: building it is O(N), the bound
	// CONTRACT_COMPLEXITY sets for the degraded path, and each manifest that
	// vanished mid-scan costs only O(log N) more.
	byNewest := newestFirst[ID](entries)
	heap.Init(&byNewest)
	for byNewest.Len() > 0 {
		e := heap.Pop(&byNewest).(manifestEntry[ID])
		s, err := h.loadUnexpired(ctx, e)
		if errors.Is(err, h.errs.NotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to load latest snapshot: %w", err)
		}
		return s, nil
	}
	return nil, h.errs.NoSnapshots
}

// loadUnexpired loads entry e unless retention expired it: an interrupted
// expiry can leave a partition copy of the newest snapshot, which must
// never become the head. A passed-over entry reads as errs.NotFound.
func (h *History[ID, M]) loadUnexpired(ctx context.Context, e manifestEntry[ID]) (*Snapshot[ID, M], error) {
	live, err := h.kind.NotExpired(ctx, e.id)
	if err != nil {
		return nil, err
	}
	if !live {
		return nil, fmt.Errorf("lode: snapshot %s is expired: %w", e.id, h.errs.NotFound)
	}
	return h.loadEntry(ctx, e)
}

// newestFirst is a max-heap of manifest entries by ID (container/heap).
type newestFirst[ID ~string] []manifestEntry[ID]

func (q newestFirst[ID]) Len() int           { return len(q) }
func (q newestFirst[ID]) Less(i, j int) bool { return q[i].id > q[j].id }
func (q newestFirst[ID]) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *newestFirst[ID]) Push(x any)        { *q = append(*q, x.(manifestEntry[ID])) }
func (q *newestFirst[ID]) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// -----------------------------------------------------------------------------
// The head pointer
// -----------------------------------------------------------------------------

// ReadPointer returns the ID the persisted pointer names, errs.NotFound when
// the pointer is absent or empty, and errs.Removed when it holds
// RemovedPointer and the container passed errs.Removed.
func (h *History[ID, M]) ReadPointer(ctx context.Context) (ID, error) {
	content, err := readPointer(ctx, h.store, h.pointerPath)
	if err != nil {
		return "", err
	}
	id := ID(strings.TrimSpace(content))
	if id == "" {
		return "", h.errs.NotFound
	}
	if h.errs.Removed != nil && id == RemovedPointer {
		return "", h.errs.Removed
	}
	return id, nil
}

// readPointer returns the pointer's content as stored.
func readPointer(ctx context.Context, store Store, pointerPath string) (string, error) {
	rc, err := store.Get(ctx, pointerPath)
	if err != nil {
		return "", fmt.Errorf("failed to read latest pointer: %w", err)
	}
	defer func() { _ = rc.Close() }() // read-only stream
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("lode: failed to read latest pointer: %w", err)
	}
	return string(data), nil
}

// Remove replaces the pointer at pointerPath with RemovedPointer through
// CompareAndSwap against the content it read, so a commit that races the
// removal fails its own swap and, on retry, reads the removal. It returns
// the content it replaced: "" when the pointer was absent, and
// RemovedPointer when the pointer already held it, in which case nothing
// is written. A conflict is retried as a commit's is, and
// errs.SnapshotConflict is returned once cfg's attempts are spent. The
// caller removes the history's other objects afterwards and never the
// pointer.
func Remove(ctx context.Context, store Store, pointerPath string, errs Errors, cfg *retry.Config) (string, error) {
	var lastErr error
	// n reaches cfg.MaxAttempts() and stops there: neither MaxAttempts()+1
	// nor n++ past it is computed, so the largest retry count neither
	// overflows into zero attempts nor into an endless loop.
	for n := 0; ; n++ {
		if n > 0 {
			if err := cfg.Backoff(ctx, n); err != nil {
				return "", err
			}
		}
		content, err := readPointer(ctx, store, pointerPath)
		if errors.Is(err, errs.NotFound) {
			content = "" // CompareAndSwap creates the pointer
		} else if err != nil {
			return "", err
		}
		previous := strings.TrimSpace(content)
		if previous == RemovedPointer {
			return previous, nil
		}
		err = store.CompareAndSwap(ctx, pointerPath, content, RemovedPointer)
		if err == nil {
			return previous, nil
		}
		if !errors.Is(err, errs.SnapshotConflict) {
			return "", fmt.Errorf("failed to replace latest pointer: %w", err)
		}
		lastErr = err
		if n >= cfg.MaxAttempts() {
			return "", lastErr
		}
	}
}

// CommittedHead returns the head retention must never remove: the snapshot
// the pointer names ("" for a history with no snapshot at all). known is a
// head an earlier call confirmed; a pointer still naming it costs no
// further call. A pointer naming a snapshot with no manifest is a commit in
// flight, whose parent the pointer no longer names: errs.HeadUncommitted.
func (h *History[ID, M]) CommittedHead(ctx context.Context, known ID) (ID, error) {
	id, err := h.ReadPointer(ctx)
	if errors.Is(err, h.errs.NotFound) {
		return "", h.pointerless(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("failed to resolve head: %w", err)
	}
	if id == known {
		return id, nil
	}
	return id, h.ConfirmCommitted(ctx, id)
}

// ConfirmCommitted returns nil when snapshot id, read from the pointer, has
// a manifest, and errs.HeadUncommitted when it has none: the commit is
// pending, and the pointer no longer names its parent. A pointer that does
// not contain a snapshot ID is corrupt and is never confirmed.
func (h *History[ID, M]) ConfirmCommitted(ctx context.Context, id ID) error {
	// A corrupt pointer must not reach a path or stand for the head: Lode
	// writes only generated IDs there, which are path-safe.
	if !IsGeneratedID(id) {
		return fmt.Errorf("lode: latest pointer holds %q, which is not a snapshot ID", id)
	}
	committed, err := h.hasManifest(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to resolve head: %w", err)
	}
	if !committed {
		return fmt.Errorf("lode: latest pointer names %s: %w", id, h.errs.HeadUncommitted)
	}
	return nil
}

// pointerless is nil for a history with no snapshot and errs.HeadUncommitted
// otherwise. Every commit writes the pointer, so it is missing beside a
// snapshot only in a history written before the pointer existed, after a
// hand edit, or after a failed Put in lode.SingleProcess's replacement. In none of these can the head be told safely: a
// clock that rolled back may have given the last commit a smaller ID.
func (h *History[ID, M]) pointerless(ctx context.Context) error {
	entries, err := h.manifests(ctx)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	return fmt.Errorf("lode: latest pointer is absent: %w", h.errs.HeadUncommitted)
}

// hasManifest reports whether snapshot id has any manifest: its canonical
// one (one Exists), or, for data written before canonical manifests, any
// listed copy (one List, only on that miss).
func (h *History[ID, M]) hasManifest(ctx context.Context, id ID) (bool, error) {
	exists, err := h.store.Exists(ctx, h.kind.ManifestPath(id))
	if err != nil || exists {
		return exists, err
	}
	entries, err := h.manifests(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(entries, func(e manifestEntry[ID]) bool { return e.id == id }), nil
}

// writePointer persists id as the pointer through a CAS against expected,
// the pointer content the writer believes is current.
func (h *History[ID, M]) writePointer(ctx context.Context, expected string, id ID) error {
	return h.store.CompareAndSwap(ctx, h.pointerPath, expected, string(id))
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
// loadable snapshot, otherwise by scan. A failed read of the pointer or of
// its manifest is returned unless the store reports the object as not found;
// a scan in its place could return the head's parent (CONTRACT_CORE §Head
// Pointer). A missing pointer is then created
// from the scan result; an existing pointer is never replaced. A pointer that
// names a snapshot without a manifest may belong to a pending commit
// (Advance writes the pointer first), and replacing it would revert the head
// past that commit (CONTRACT_CORE §Head Pointer). The head is retained in
// memory only while the pointer is known to name it; otherwise the memory is
// discarded, so the next Resolve consults storage and adopts the pending
// commit as parent.
func (h *History[ID, M]) Latest(ctx context.Context) (*Snapshot[ID, M], error) {
	s, pointerMissing, err := h.resolveHead(ctx)
	if err != nil {
		return nil, err
	}
	if pointerMissing {
		h.createPointer(ctx, s.ID)
	}
	return s, nil
}

// Head is Latest for a reader that never writes: it resolves the head the
// same way but never creates a missing pointer (CONTRACT_CORE §Head
// Pointer), so it needs no CompareAndSwap.
func (h *History[ID, M]) Head(ctx context.Context) (*Snapshot[ID, M], error) {
	s, _, err := h.resolveHead(ctx)
	return s, err
}

// resolveHead resolves the head through the pointer, or by scan when the pointer
// names no loadable snapshot, and reports whether the pointer was missing.
func (h *History[ID, M]) resolveHead(ctx context.Context) (s *Snapshot[ID, M], pointerMissing bool, err error) {
	id, err := h.ReadPointer(ctx)
	pointerMissing = errors.Is(err, h.errs.NotFound)
	if err == nil {
		s, loadErr := h.Snapshot(ctx, id)
		if loadErr == nil {
			h.remember(id, string(id))
			return s, false, nil
		}
		if !errors.Is(loadErr, h.errs.NotFound) {
			err = fmt.Errorf("failed to load latest snapshot: %w", loadErr)
		}
	}
	// The pointer confirms no loadable head, so the remembered head may be
	// stale; the next Resolve must consult storage.
	h.Forget()
	if err != nil && !pointerMissing {
		return nil, false, err
	}

	s, err = h.scan(ctx)
	if err != nil {
		return nil, false, err
	}
	return s, pointerMissing, nil
}

// createPointer creates a missing pointer naming head, the scan's result.
// Best-effort: a conflict means a concurrent commit created it first, and
// any other failure leaves the next Latest to scan again.
func (h *History[ID, M]) createPointer(ctx context.Context, head ID) {
	if h.writePointer(ctx, "", head) == nil {
		h.remember(head, string(head))
	}
}

// Resolve returns the ID a new commit parents on ("" for the first) and the
// pointer content a CAS advance must expect. Order: the in-process memory
// (authoritative within one writer), then the pointer (one Get on cold
// start), then scan. The snapshot that the pointer names is the parent even
// before its manifest is persisted: Advance writes the pointer first, so the
// snapshot may belong to a pending commit, and adopting any other parent
// would fork the history (CONTRACT_CORE §Head Pointer).
func (h *History[ID, M]) Resolve(ctx context.Context) (parent ID, expected string, err error) {
	id, _, expected, err := h.resolve(ctx)
	return id, expected, err
}

// ResolveParent is Resolve for containers whose commit needs the parent's
// manifest (a Volume merges its blocks): the parent is loaded, never
// re-fetched, and a remembered head whose manifest is gone is forgotten so
// storage is consulted instead. parent is nil for the first commit. A
// pointer that names a snapshot without a persisted manifest yields its
// staged copy (stagedParent).
func (h *History[ID, M]) ResolveParent(ctx context.Context) (parent *Snapshot[ID, M], expected string, err error) {
	for {
		fromMemory := h.head != ""
		id, scanned, expected, err := h.resolve(ctx)
		if err != nil || id == "" || scanned != nil {
			return scanned, expected, err
		}
		parent, err := h.Snapshot(ctx, id)
		switch {
		case err == nil:
			return parent, expected, nil
		case !errors.Is(err, h.errs.NotFound) && fromMemory:
			return nil, "", fmt.Errorf("failed to load parent snapshot: %w", err)
		case !errors.Is(err, h.errs.NotFound):
			return nil, "", fmt.Errorf("failed to load snapshot: %w", err)
		case !fromMemory:
			return h.stagedParent(ctx, id, expected)
		}
		h.Forget() // the remembered head is gone; consult storage
	}
}

// stagedParent loads snapshot id, which the pointer names but whose
// canonical manifest is absent, from the staged copy its commit persisted
// before advancing the pointer: the commit is pending, or abandoned by a
// writer that terminated. A staged copy that is absent too was removed after
// the canonical manifest was persisted, which is read again; without either
// (a commit staged nothing, or the pointer was edited by hand) the parent's
// blocks cannot be merged, so the commit conflicts and retries as after any
// other conflict.
func (h *History[ID, M]) stagedParent(ctx context.Context, id ID, expected string) (*Snapshot[ID, M], string, error) {
	if h.stager == nil {
		return nil, "", h.unmerged(id)
	}
	staged, err := h.LoadAt(ctx, id, h.stager.StagedPath(id))
	if err == nil {
		return staged, expected, nil
	}
	if !errors.Is(err, h.errs.NotFound) {
		return nil, "", fmt.Errorf("failed to load staged snapshot: %w", err)
	}
	canonical, err := h.Snapshot(ctx, id)
	if errors.Is(err, h.errs.NotFound) {
		return nil, "", h.unmerged(id)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to load snapshot: %w", err)
	}
	return canonical, expected, nil
}

// unmerged is the conflict for a parent whose manifest no copy holds.
func (h *History[ID, M]) unmerged(id ID) error {
	return fmt.Errorf("lode: latest pointer names %s, whose manifest has not been persisted: %w", id, h.errs.SnapshotConflict)
}

// resolve walks memory → pointer → scan. scanned is the snapshot the scan
// loaded, nil on the memory and pointer paths, which load nothing. A
// pointer that does not contain a snapshot ID (a manual edit) cannot name
// a commit, so the scan determines the parent; the pointer's content remains
// the value that CAS must expect. A failed pointer read other than not-found
// is returned, because the scan may select the head's parent.
func (h *History[ID, M]) resolve(ctx context.Context) (id ID, scanned *Snapshot[ID, M], expected string, err error) {
	if h.head != "" {
		return h.head, nil, h.pointer, nil
	}

	var observed string
	pointed, err := h.ReadPointer(ctx)
	if err != nil && !errors.Is(err, h.errs.NotFound) {
		return "", nil, "", err
	}
	if err == nil {
		if IsGeneratedID(pointed) {
			return pointed, nil, string(pointed), nil
		}
		observed = string(pointed)
	}

	s, err := h.scan(ctx)
	if errors.Is(err, h.errs.NoSnapshots) {
		return "", nil, observed, nil
	}
	if err != nil {
		return "", nil, "", fmt.Errorf("failed to get latest snapshot: %w", err)
	}
	return s.ID, s, observed, nil
}

// ErrManifestUncertain marks a commit that failed after its pointer advanced,
// while it was writing manifests. A failed Put does not say whether the
// object was stored (a lost response stores it), so the snapshot may be
// visible, and nothing it references may be deleted. Errors carrying it keep
// the message of the underlying failure.
var ErrManifestUncertain = errors.New("the commit's manifest may have been persisted")

// manifestUncertain wraps a manifest write failure so that it matches
// ErrManifestUncertain without changing its message.
type manifestUncertain struct{ err error }

func (e manifestUncertain) Error() string        { return e.err.Error() }
func (e manifestUncertain) Unwrap() error        { return e.err }
func (e manifestUncertain) Is(target error) bool { return target == ErrManifestUncertain }

// Advance commits a snapshot: the staged copy when the kind stages, the
// pointer (CAS against expected), then every manifest path, then the removal
// of the staged copy. The pointer precedes the manifests so a crash between
// them leaves a pending commit that the next commit adopts as its parent
// (CONTRACT_CORE §Head Pointer), never a committed manifest the pointer
// skips; the staged copy precedes the pointer so that parent can be loaded
// (stagedParent). A failure after the pointer advanced matches
// ErrManifestUncertain. On success the new head is remembered.
func (h *History[ID, M]) Advance(ctx context.Context, expected string, id ID, manifest *M, paths []string) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("lode: failed to marshal %s: %w", h.noun, err)
	}
	staged := h.stagedPath(id, expected)
	if err := h.stage(ctx, staged, data); err != nil {
		return fmt.Errorf("failed to stage %s: %w", h.noun, err)
	}
	if err := h.writePointer(ctx, expected, id); err != nil {
		h.discardStaged(ctx, id, staged)
		return fmt.Errorf("failed to update latest pointer: %w", err)
	}
	for _, p := range paths {
		if err := h.store.Put(ctx, p, bytes.NewReader(data)); err != nil {
			return manifestUncertain{fmt.Errorf("failed to write %s: %w", h.noun, err)}
		}
	}
	h.unstage(ctx, staged)
	h.remember(id, string(id))
	return nil
}

// stagedPath is the staged copy of snapshot id's manifest, "" when the kind
// does not stage a commit that expects the given pointer content.
func (h *History[ID, M]) stagedPath(id ID, expected string) string {
	if h.stager == nil || !h.stager.StagesCommit(expected) {
		return ""
	}
	return h.stager.StagedPath(id)
}

// LoadStaged loads snapshot id from its staged copy. errs.NotFound when the
// kind stages nothing or no staged copy exists.
func (h *History[ID, M]) LoadStaged(ctx context.Context, id ID) (*Snapshot[ID, M], error) {
	if h.stager == nil {
		return nil, h.errs.NotFound
	}
	return h.LoadAt(ctx, id, h.stager.StagedPath(id))
}

// Newest returns the snapshot with the largest ID that has an unexpired
// manifest, by scan, without reading or creating the pointer. errs.NoSnapshots
// when there is none.
func (h *History[ID, M]) Newest(ctx context.Context) (*Snapshot[ID, M], error) {
	return h.scan(ctx)
}

// stage persists the staged copy at staged, when the kind stages.
func (h *History[ID, M]) stage(ctx context.Context, staged string, data []byte) error {
	if staged == "" {
		return nil
	}
	return h.store.Put(ctx, staged, bytes.NewReader(data))
}

// unstage removes the staged copy once the canonical manifest is persisted.
// Best-effort: a copy that remains is never listed, and it duplicates a
// persisted manifest.
func (h *History[ID, M]) unstage(ctx context.Context, staged string) {
	if staged != "" {
		_ = h.store.Delete(ctx, staged)
	}
}

// discardStaged removes the staged copy (at staged) of a commit whose pointer update
// failed, but only when the pointer names another snapshot. A failed update
// may still have been applied (a store that retried a CAS whose response was
// lost reports a conflict), and the staged copy is then the only manifest of
// a committed snapshot. Once the pointer names another snapshot, every
// commit that adopted id as parent has loaded the copy already. Best-effort:
// a copy that remains is never listed.
func (h *History[ID, M]) discardStaged(ctx context.Context, id ID, staged string) {
	if staged == "" {
		return
	}
	if pointed, err := h.ReadPointer(ctx); err == nil && pointed != id {
		_ = h.store.Delete(ctx, staged)
	}
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
		if err := h.removed(ctx); err != nil {
			return err
		}
		lastErr = err
	}
	return lastErr
}

// removed reports the removal of a removable history whose commit just
// conflicted. A writer that remembers its head commits without reading the
// pointer, so its swap is where it meets the marker: one read on the
// conflict path tells a removal from a concurrent commit, and the writer
// then returns errs.Removed whatever its retries. Nil for a history that
// cannot be removed, or whose pointer holds anything else. A read that
// fails otherwise is returned, as every read of the pointer is
// (CONTRACT_CORE §Head Pointer): the conflict may be the removal's.
func (h *History[ID, M]) removed(ctx context.Context) error {
	if h.errs.Removed == nil {
		return nil
	}
	_, err := h.ReadPointer(ctx)
	switch {
	case err == nil, errors.Is(err, h.errs.NotFound):
		return nil
	case errors.Is(err, h.errs.Removed):
		h.Forget()
		return err
	default:
		return fmt.Errorf("failed to resolve head after a conflict: %w", err)
	}
}
