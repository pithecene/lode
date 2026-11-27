package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
)

// tombstone is the persisted record of an expiry (CONTRACT_RETENTION
// §Tombstone). It carries the snapshot's inventory, copied from the
// manifest before anything is deleted, so every later step works from the
// tombstone rather than from manifests that may already be gone, and the
// manifest's commit instant, which with the inventory tells this snapshot
// from a later one that reused its ID.
type tombstone struct {
	SnapshotID        SnapshotID  `json:"snapshot_id"`
	RequestedAt       time.Time   `json:"requested_at"`
	ManifestCreatedAt time.Time   `json:"manifest_created_at"`
	Objects           []ObjectRef `json:"objects"`
}

// UnmarshalJSON decodes format 1.2.0 tombstones ("objects") as well as
// pre-1.2.0 tombstones ("files").
func (t *tombstone) UnmarshalJSON(data []byte) error {
	type tombstoneAlias tombstone
	var wire struct {
		tombstoneAlias
		LegacyFiles []ObjectRef `json:"files"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*t = tombstone(wire.tombstoneAlias)
	if t.Objects == nil && wire.LegacyFiles != nil {
		t.Objects = wire.LegacyFiles
	}
	return nil
}

// newTombstone records the expiry of snapshot id from its manifest m.
func newTombstone(id SnapshotID, m *Manifest) tombstone {
	objects := m.Objects
	if objects == nil {
		objects = []ObjectRef{} // an inventory is never null, so absence means corruption
	}
	return tombstone{SnapshotID: id, RequestedAt: time.Now().UTC(), ManifestCreatedAt: m.CreatedAt, Objects: objects}
}

// sameSnapshot reports whether t and o record the same snapshot: the same
// commit instant and the same inventory, paths and sizes.
func (t tombstone) sameSnapshot(o tombstone) bool {
	return t.ManifestCreatedAt.Equal(o.ManifestCreatedAt) &&
		slices.EqualFunc(t.Objects, o.Objects, func(x, y ObjectRef) bool { return x.Path == y.Path && x.SizeBytes == y.SizeBytes })
}

// describes reports whether t is the record of manifest m
// (CONTRACT_RETENTION §Tombstone).
func (t tombstone) describes(m *Manifest) bool {
	return t.sameSnapshot(tombstone{ManifestCreatedAt: m.CreatedAt, Objects: m.Objects})
}

// tombstones is a dataset's tombstone directory. Tombstones are created
// once and never rewritten or removed.
type tombstones struct {
	store   Store
	paths   *pathLayout
	dataset DatasetID
}

func (d *dataset) tombstones() tombstones {
	return tombstones{store: d.store, paths: d.layout, dataset: d.id}
}

// path is where snapshot id's tombstone lives. ok is false unless id is in
// the generated form: only such an ID names a tombstone, and no other
// caller-supplied string is ever turned into a store path.
func (ts tombstones) path(id SnapshotID) (p string, ok bool) {
	if !history.IsGeneratedID(id) {
		return "", false
	}
	return ts.paths.tombstonePath(ts.dataset, id), true
}

// record creates the tombstone, or accepts the one an earlier or concurrent
// run recorded for the same snapshot with the same inventory.
// It reports whether this call wrote the tombstone, false when another run's
// matching tombstone won the create.
func (ts tombstones) record(ctx context.Context, t tombstone) (bool, error) {
	p, ok := ts.path(t.SnapshotID)
	if !ok {
		return false, errUntombstonable(t.SnapshotID)
	}
	data, err := json.Marshal(t)
	if err != nil {
		return false, fmt.Errorf("lode: failed to marshal tombstone: %w", err)
	}
	err = ts.create(ctx, p, data)
	if errors.Is(err, ErrSnapshotConflict) || errors.Is(err, ErrPathExists) {
		return false, ts.matchRecorded(ctx, p, t)
	}
	if err != nil {
		return false, fmt.Errorf("failed to write tombstone %s: %w", p, err)
	}
	return true, nil
}

// matchRecorded accepts the tombstone that won the create at p only if it
// records the same snapshot as t: another must never stand as the record of
// the objects this run deletes.
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
	prefix := ts.paths.tombstonesPrefix(ts.dataset)
	keys, err := ts.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list tombstones: %w", err)
	}
	return slices.DeleteFunc(keys, func(k string) bool { return !isTombstoneName(k, prefix) }), nil
}

// errUntombstonable is the error for a snapshot whose ID is not in the
// generated form: no tombstone path can be formed for it, so it is never
// expired, on a dry run or a real one.
func errUntombstonable(id SnapshotID) error {
	return fmt.Errorf("lode: snapshot ID %q is not in the generated form; it cannot be tombstoned", id)
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
var errInvalidTombstone = errors.New("invalid tombstone")

// recorded reads the tombstone at a listed key. errInvalidTombstone reports
// one whose key is not canonical or that does not record the snapshot its
// own path names with a valid inventory.
func (ts tombstones) recorded(ctx context.Context, key string) (tombstone, error) {
	id := SnapshotID(strings.TrimPrefix(key, ts.paths.tombstonesPrefix(ts.dataset)))
	if !ts.isCanonicalPath(key, id) {
		return tombstone{}, fmt.Errorf("lode: %w: %s is not a canonical tombstone path", errInvalidTombstone, key)
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
func (ts tombstones) isCanonicalPath(key string, id SnapshotID) bool {
	canonical, ok := storepath.Key(key)
	p, named := ts.path(id)
	return ok && named && canonical == key && key == p
}

// create writes p atomically, only if it is absent.
func (ts tombstones) create(ctx context.Context, p string, data []byte) error {
	return ts.store.CompareAndSwap(ctx, p, "", string(data))
}

// errEmptyTombstone is a zero-length object at a tombstone path. record
// replaces it (a CAS from "" matches it), so it stands for no tombstone only
// while a manifest is left to record from.
var errEmptyTombstone = errors.New("tombstone is empty")

// lookup returns the tombstone recorded for id. ErrNotFound means there is
// none, and errEmptyTombstone reports the zero-length placeholder above.
// Anything else must be a valid tombstone of this snapshot; otherwise it is
// corruption.
func (ts tombstones) lookup(ctx context.Context, id SnapshotID) (tombstone, error) {
	p, ok := ts.path(id)
	if !ok {
		return tombstone{}, fmt.Errorf("lode: snapshot ID %q names no tombstone: %w", id, ErrNotFound)
	}
	data, err := ts.readRaw(ctx, p)
	if err != nil {
		return tombstone{}, err
	}
	if len(data) == 0 {
		return tombstone{}, fmt.Errorf("lode: %w: %s", errEmptyTombstone, p)
	}
	return validTombstone(p, data, id)
}

// validTombstone decodes data and requires it to record snapshot id, the
// manifest's commit instant, and an inventory whose every ObjectRef is valid.
func validTombstone(p string, data []byte, id SnapshotID) (tombstone, error) {
	var t tombstone
	if err := json.Unmarshal(data, &t); err != nil {
		return t, fmt.Errorf("lode: failed to decode tombstone %s: %w", p, err)
	}
	if t.SnapshotID != id {
		return t, fmt.Errorf("lode: tombstone %s records snapshot %q, not %q", p, t.SnapshotID, id)
	}
	if t.ManifestCreatedAt.IsZero() {
		return t, fmt.Errorf("lode: tombstone %s has no manifest commit instant", p)
	}
	if t.Objects == nil {
		return t, fmt.Errorf("lode: tombstone %s has no inventory", p)
	}
	if err := validateObjects(t.Objects); err != nil {
		return t, fmt.Errorf("tombstone %s has an invalid inventory: %w", p, err)
	}
	return t, nil
}

func (ts tombstones) readRaw(ctx context.Context, p string) ([]byte, error) {
	rc, err := ts.store.Get(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed to read tombstone %s: %w", p, err)
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
func (ts tombstones) expiredError(ctx context.Context, id SnapshotID) error {
	absent, err := ts.absent(ctx, id)
	if err != nil || absent {
		return err
	}
	return fmt.Errorf("lode: snapshot %s: %w", id, ErrSnapshotExpired)
}

// absent reports whether snapshot id has no valid tombstone. An ID outside
// the generated form never has one.
func (ts tombstones) absent(ctx context.Context, id SnapshotID) (bool, error) {
	// Only a valid tombstone expires a snapshot: an empty placeholder records
	// nothing, and anything else at the path is corruption, surfaced rather
	// than read as an expiry.
	_, err := ts.lookup(ctx, id)
	if errors.Is(err, ErrNotFound) || errors.Is(err, errEmptyTombstone) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check tombstone: %w", err)
	}
	return false, nil
}
