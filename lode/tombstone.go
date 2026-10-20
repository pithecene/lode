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
	"time"

	"github.com/pithecene-io/lode/internal/history"
	"github.com/pithecene-io/lode/internal/storepath"
)

// tombstone is the persisted record of an expiry (CONTRACT_RETENTION
// §Tombstone). It carries the snapshot's inventory, copied from the
// manifest before anything is deleted, so every later step works from the
// tombstone rather than from manifests that may already be gone, and the
// manifest's commit instant, which with the inventory tells this snapshot
// from a later one that reused its ID.
type tombstone struct {
	SnapshotID        DatasetSnapshotID `json:"snapshot_id"`
	RequestedAt       time.Time         `json:"requested_at"`
	ManifestCreatedAt time.Time         `json:"manifest_created_at"`
	Files             []FileRef         `json:"files"`
}

// newTombstone records the expiry of snapshot id from its manifest m.
func newTombstone(id DatasetSnapshotID, m *Manifest) tombstone {
	files := m.Files
	if files == nil {
		files = []FileRef{} // an inventory is never null, so absence means corruption
	}
	return tombstone{SnapshotID: id, RequestedAt: time.Now().UTC(), ManifestCreatedAt: m.CreatedAt, Files: files}
}

// sameSnapshot reports whether t and o record the same snapshot: the same
// commit instant and the same inventory, paths and sizes.
func (t tombstone) sameSnapshot(o tombstone) bool {
	return t.ManifestCreatedAt.Equal(o.ManifestCreatedAt) &&
		slices.EqualFunc(t.Files, o.Files, func(x, y FileRef) bool { return x.Path == y.Path && x.SizeBytes == y.SizeBytes })
}

// describes reports whether t is the record of manifest m
// (CONTRACT_RETENTION §Tombstone).
func (t tombstone) describes(m *Manifest) bool {
	return t.sameSnapshot(tombstone{ManifestCreatedAt: m.CreatedAt, Files: m.Files})
}

// tombstones is a dataset's tombstone directory. Tombstones are created
// once and never rewritten or removed.
type tombstones struct {
	store   Store
	layout  layout
	dataset DatasetID
}

func (d *dataset) tombstones() tombstones {
	return tombstones{store: d.store, layout: d.layout, dataset: d.id}
}

// path is where snapshot id's tombstone lives. ok is false unless id is in
// the generated form: only such an ID names a tombstone, and no other
// caller-supplied string is ever turned into a store path.
func (ts tombstones) path(id DatasetSnapshotID) (p string, ok bool) {
	if !history.IsGeneratedID(id) {
		return "", false
	}
	return ts.layout.tombstonePath(ts.dataset, id), true
}

// record creates the tombstone, or accepts the one an earlier or concurrent
// run recorded for the same snapshot with the same inventory.
func (ts tombstones) record(ctx context.Context, t tombstone) error {
	p, ok := ts.path(t.SnapshotID)
	if !ok {
		return fmt.Errorf("lode: snapshot ID %q is not in the generated form; it cannot be tombstoned", t.SnapshotID)
	}
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("lode: failed to marshal tombstone: %w", err)
	}
	err = ts.create(ctx, p, data)
	if errors.Is(err, ErrSnapshotConflict) || errors.Is(err, ErrPathExists) {
		return ts.matchRecorded(ctx, p, t)
	}
	if err != nil {
		return fmt.Errorf("lode: failed to write tombstone %s: %w", p, err)
	}
	return nil
}

// matchRecorded accepts the tombstone that won the create at p only if it
// records the same snapshot as t: another must never stand as the record of
// the files this run deletes.
func (ts tombstones) matchRecorded(ctx context.Context, p string, t tombstone) error {
	existing, err := ts.lookup(ctx, t.SnapshotID)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("lode: tombstone %s exists but reads as absent", p)
	}
	if err != nil {
		return err
	}
	if !existing.sameSnapshot(t) {
		return fmt.Errorf("lode: tombstone %s records another snapshot than its manifest", p)
	}
	return nil
}

// list returns the keys under the tombstone directory that name a
// snapshot (CONTRACT_RETENTION §Definition, step 1).
func (ts tombstones) list(ctx context.Context) ([]string, error) {
	prefix := ts.layout.tombstonesPrefix(ts.dataset)
	keys, err := ts.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list tombstones: %w", err)
	}
	return slices.DeleteFunc(keys, func(k string) bool { return !isTombstoneName(k, prefix) }), nil
}

// isTombstoneName reports whether a key below the tombstone directory names
// a snapshot ID exactly as Lode generates one. Anything else there (the
// filesystem store's CAS .lock companions) is not a tombstone at all.
func isTombstoneName(key, prefix string) bool {
	return history.IsGeneratedID(strings.TrimPrefix(key, prefix))
}

// errInvalidTombstone marks a listed tombstone Lode cannot trust: a key
// that is not canonical, or content that fails validation. It settles
// nothing (CONTRACT_RETENTION §Definition).
var errInvalidTombstone = errors.New("lode: invalid tombstone")

// recorded reads the tombstone at a listed key. errInvalidTombstone reports
// one whose key is not canonical or that does not record the snapshot its
// own path names with a valid inventory.
func (ts tombstones) recorded(ctx context.Context, key string) (tombstone, error) {
	id := DatasetSnapshotID(strings.TrimPrefix(key, ts.layout.tombstonesPrefix(ts.dataset)))
	if !ts.isCanonicalPath(key, id) {
		return tombstone{}, fmt.Errorf("%w: %s is not a canonical tombstone path", errInvalidTombstone, key)
	}
	data, err := ts.readRaw(ctx, key)
	if err != nil {
		return tombstone{}, err
	}
	t, err := validTombstone(key, data, id)
	if err != nil {
		return tombstone{}, fmt.Errorf("%w: %w", errInvalidTombstone, err)
	}
	return t, nil
}

// isCanonicalPath reports whether key is a canonical store key and exactly
// the path of snapshot id's tombstone.
func (ts tombstones) isCanonicalPath(key string, id DatasetSnapshotID) bool {
	canonical, ok := storepath.Key(key)
	p, named := ts.path(id)
	return ok && named && canonical == key && key == p
}

// create writes p only if it is absent: atomically when the store is a
// ConditionalWriter, otherwise with Put's best-effort create-only guarantee.
func (ts tombstones) create(ctx context.Context, p string, data []byte) error {
	if cw, ok := ts.store.(ConditionalWriter); ok {
		return cw.CompareAndSwap(ctx, p, "", string(data))
	}
	return ts.store.Put(ctx, p, bytes.NewReader(data))
}

// errEmptyTombstone is a zero-length object at a tombstone path of a
// ConditionalWriter. record replaces it (a CAS from "" matches it), so it
// stands for no tombstone only while a manifest is left to record from.
var errEmptyTombstone = errors.New("lode: tombstone is empty")

// lookup returns the tombstone recorded for id. ErrNotFound means there is
// none, and errEmptyTombstone reports the zero-length placeholder above.
// Anything else must be a valid tombstone of this snapshot; otherwise it is
// corruption.
func (ts tombstones) lookup(ctx context.Context, id DatasetSnapshotID) (tombstone, error) {
	p, ok := ts.path(id)
	if !ok {
		return tombstone{}, fmt.Errorf("lode: snapshot ID %q names no tombstone: %w", id, ErrNotFound)
	}
	data, err := ts.readRaw(ctx, p)
	if err != nil {
		return tombstone{}, err
	}
	if _, cas := ts.store.(ConditionalWriter); cas && len(data) == 0 {
		return tombstone{}, fmt.Errorf("%w: %s", errEmptyTombstone, p)
	}
	return validTombstone(p, data, id)
}

// validTombstone decodes data and requires it to record snapshot id, the
// manifest's commit instant, and an inventory whose every FileRef is valid.
func validTombstone(p string, data []byte, id DatasetSnapshotID) (tombstone, error) {
	var t tombstone
	if err := json.Unmarshal(data, &t); err != nil {
		return t, fmt.Errorf("lode: failed to decode tombstone %s: %w", p, err)
	}
	for _, check := range []struct {
		bad     bool
		problem string
	}{
		{t.SnapshotID != id, fmt.Sprintf("records snapshot %q, not %q", t.SnapshotID, id)},
		{t.ManifestCreatedAt.IsZero(), "has no manifest commit instant"},
		{t.Files == nil, "has no inventory"},
	} {
		if check.bad {
			return t, fmt.Errorf("lode: tombstone %s %s", p, check.problem)
		}
	}
	if err := validateFiles(t.Files); err != nil {
		return t, fmt.Errorf("lode: tombstone %s has an invalid inventory: %w", p, err)
	}
	return t, nil
}

func (ts tombstones) readRaw(ctx context.Context, p string) ([]byte, error) {
	rc, err := ts.store.Get(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to read tombstone %s: %w", p, err)
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("lode: failed to read tombstone %s: %w", p, err)
	}
	return data, nil
}

// expiredError is ErrSnapshotExpired when snapshot id has a tombstone, nil
// when it has none. Only meaningful once its canonical manifest is missing.
func (ts tombstones) expiredError(ctx context.Context, id DatasetSnapshotID) error {
	absent, err := ts.absent(ctx, id)
	if err != nil || absent {
		return err
	}
	return fmt.Errorf("lode: snapshot %s: %w", id, ErrSnapshotExpired)
}

// absent reports whether snapshot id has no valid tombstone. An ID outside
// the generated form never has one.
func (ts tombstones) absent(ctx context.Context, id DatasetSnapshotID) (bool, error) {
	// Only a valid tombstone expires a snapshot: an empty placeholder records
	// nothing, and anything else at the path is corruption, surfaced rather
	// than read as an expiry.
	_, err := ts.lookup(ctx, id)
	if errors.Is(err, ErrNotFound) || errors.Is(err, errEmptyTombstone) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("lode: failed to check tombstone: %w", err)
	}
	return false, nil
}
