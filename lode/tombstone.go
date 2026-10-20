package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// tombstone is the persisted record of an expiry (CONTRACT_RETENTION
// §Tombstone). It carries the snapshot's inventory, copied from the
// manifest before anything is deleted, so every later step works from the
// tombstone rather than from manifests that may already be gone.
type tombstone struct {
	SnapshotID  DatasetSnapshotID `json:"snapshot_id"`
	RequestedAt time.Time         `json:"requested_at"`
	Files       []FileRef         `json:"files"`
}

func newTombstone(id DatasetSnapshotID, files []FileRef) tombstone {
	if files == nil {
		files = []FileRef{} // an inventory is never null, so absence means corruption
	}
	return tombstone{SnapshotID: id, RequestedAt: time.Now().UTC(), Files: files}
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
	if _, generated := snapshotInstant(id); !generated {
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
// records the inventory t would have: a different one must never stand as
// the record of the files this run deletes.
func (ts tombstones) matchRecorded(ctx context.Context, p string, t tombstone) error {
	existing, err := ts.lookup(ctx, t.SnapshotID)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("lode: tombstone %s exists but reads as absent", p)
	}
	if err != nil {
		return err
	}
	if !sameInventory(existing.Files, t.Files) {
		return fmt.Errorf("lode: tombstone %s records a different inventory than its manifest", p)
	}
	return nil
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

// validTombstone decodes data and requires it to record snapshot id and to
// carry an inventory whose every FileRef is valid.
func validTombstone(p string, data []byte, id DatasetSnapshotID) (tombstone, error) {
	var t tombstone
	if err := json.Unmarshal(data, &t); err != nil {
		return t, fmt.Errorf("lode: failed to decode tombstone %s: %w", p, err)
	}
	if t.SnapshotID != id {
		return t, fmt.Errorf("lode: tombstone %s records snapshot %q, not %q", p, t.SnapshotID, id)
	}
	if t.Files == nil {
		return t, fmt.Errorf("lode: tombstone %s has no inventory", p)
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
	p, ok := ts.path(id)
	if !ok {
		return nil
	}
	exists, err := ts.store.Exists(ctx, p)
	if err != nil {
		return fmt.Errorf("lode: failed to check tombstone: %w", err)
	}
	if exists {
		return fmt.Errorf("lode: snapshot %s: %w", id, ErrSnapshotExpired)
	}
	return nil
}
