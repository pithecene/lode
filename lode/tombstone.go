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
// §Tombstone). It records intent, not outcome: the object count and bytes
// are the manifest's whole Files[], not what was deleted.
type tombstone struct {
	SnapshotID  DatasetSnapshotID `json:"snapshot_id"`
	RequestedAt time.Time         `json:"requested_at"`
	Objects     int               `json:"objects"`
	Bytes       int64             `json:"bytes"`
}

func newTombstone(id DatasetSnapshotID, planned []FileRef) tombstone {
	return tombstone{SnapshotID: id, RequestedAt: time.Now().UTC(), Objects: len(planned), Bytes: totalBytes(planned)}
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

func (ts tombstones) path(id DatasetSnapshotID) string {
	return ts.layout.tombstonePath(ts.dataset, id)
}

// record creates the tombstone, or confirms that an existing one records
// the same snapshot: the same expiry, repeated or concurrent.
func (ts tombstones) record(ctx context.Context, t tombstone) error {
	p := ts.path(t.SnapshotID)
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("lode: failed to marshal tombstone: %w", err)
	}
	err = ts.create(ctx, p, data)
	if errors.Is(err, ErrSnapshotConflict) || errors.Is(err, ErrPathExists) {
		return ts.confirm(ctx, p, t.SnapshotID)
	}
	if err != nil {
		return fmt.Errorf("lode: failed to write tombstone %s: %w", p, err)
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

// check is record without the write: it fails exactly where record would.
// An absent tombstone would be created, and so would a zero-length one on a
// ConditionalWriter (a CAS from "" matches it); anything else must record
// this snapshot.
func (ts tombstones) check(ctx context.Context, id DatasetSnapshotID) error {
	p := ts.path(id)
	data, err := ts.readRaw(ctx, p)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, cas := ts.store.(ConditionalWriter); cas && len(data) == 0 {
		return nil
	}
	return confirmTombstone(p, data, id)
}

// confirm fails unless the tombstone at p records snapshot id.
func (ts tombstones) confirm(ctx context.Context, p string, id DatasetSnapshotID) error {
	data, err := ts.readRaw(ctx, p)
	if err != nil {
		return err
	}
	return confirmTombstone(p, data, id)
}

func confirmTombstone(p string, data []byte, id DatasetSnapshotID) error {
	t, err := decodeTombstone(p, data)
	if err != nil {
		return err
	}
	if t.SnapshotID != id {
		return fmt.Errorf("lode: tombstone %s records snapshot %q, not %q", p, t.SnapshotID, id)
	}
	return nil
}

func (ts tombstones) read(ctx context.Context, p string) (tombstone, error) {
	data, err := ts.readRaw(ctx, p)
	if err != nil {
		return tombstone{}, err
	}
	return decodeTombstone(p, data)
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

func decodeTombstone(p string, data []byte) (tombstone, error) {
	var t tombstone
	if err := json.Unmarshal(data, &t); err != nil {
		return tombstone{}, fmt.Errorf("lode: failed to decode tombstone %s: %w", p, err)
	}
	return t, nil
}

// expiredError is ErrSnapshotExpired when snapshot id has a tombstone, nil
// when it has none. Only meaningful once its canonical manifest is missing.
func (ts tombstones) expiredError(ctx context.Context, id DatasetSnapshotID) error {
	exists, err := ts.store.Exists(ctx, ts.path(id))
	if err != nil {
		return fmt.Errorf("lode: failed to check tombstone: %w", err)
	}
	if exists {
		return fmt.Errorf("lode: snapshot %s: %w", id, ErrSnapshotExpired)
	}
	return nil
}
