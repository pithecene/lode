package lode

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// Test stores
// -----------------------------------------------------------------------------

// hookStore wraps a Store with hooks. Embedding the interface hides any
// ConditionalWriter of the inner store, so it is also the non-CAS store.
type hookStore struct {
	Store
	onList   func(prefix string, keys []string)
	onDelete func(path string) error
}

func (h *hookStore) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := h.Store.List(ctx, prefix)
	if err == nil && h.onList != nil {
		h.onList(prefix, keys)
	}
	return keys, err
}

func (h *hookStore) Delete(ctx context.Context, p string) error {
	if h.onDelete != nil {
		if err := h.onDelete(p); err != nil {
			return err
		}
	}
	return h.Store.Delete(ctx, p)
}

// casHookStore is a hookStore that keeps the inner store's CompareAndSwap.
type casHookStore struct{ *hookStore }

func (c casHookStore) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	return c.Store.(ConditionalWriter).CompareAndSwap(ctx, p, expected, replacement)
}

// racingStore records a tombstone for the same snapshot just before the
// first tombstone CAS, as a concurrent expiry of the same ID would. With
// rival set, the raced tombstone is rival instead of this run's own.
type racingStore struct {
	*hookStore
	rival string
	raced bool
}

func (r *racingStore) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	cw := r.Store.(ConditionalWriter)
	if !r.raced && strings.Contains(p, "/expired/") {
		r.raced = true
		if err := cw.CompareAndSwap(ctx, p, "", cmp.Or(r.rival, replacement)); err != nil {
			return err
		}
	}
	return cw.CompareAndSwap(ctx, p, expected, replacement)
}

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

func newHiveRetention(t *testing.T, store Store) Dataset {
	t.Helper()
	ds, err := NewDataset("events", NewMemoryFactoryFrom(store),
		WithHiveLayout("run_id", "event_type"), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func newDefaultRetention(t *testing.T, store Store, opts ...Option) Dataset {
	t.Helper()
	ds, err := NewDataset("events", NewMemoryFactoryFrom(store), append([]Option{WithCodec(NewJSONLCodec())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

// writeRun commits one snapshot whose records span the given event types of
// one run.
func writeRun(t *testing.T, ds Dataset, run string, eventTypes ...string) *DatasetSnapshot {
	t.Helper()
	records := make([]any, 0, len(eventTypes))
	for _, et := range eventTypes {
		records = append(records, D{"run_id": run, "event_type": et})
	}
	snap, err := ds.Write(t.Context(), records, Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func writeRecord(t *testing.T, ds Dataset, n int) *DatasetSnapshot {
	t.Helper()
	snap, err := ds.Write(t.Context(), R(D{"n": n}), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func storeKeys(t *testing.T, store Store) []string {
	t.Helper()
	keys, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// assertGone checks every manifest and data object of snap is absent.
func assertGone(t *testing.T, store Store, ds Dataset, snap *DatasetSnapshot) {
	t.Helper()
	for _, k := range storeKeys(t, store) {
		if strings.Contains(k, "/"+string(snap.ID)+"/") {
			t.Errorf("object of expired snapshot remains: %s", k)
		}
	}
	_, err := ds.Snapshot(t.Context(), snap.ID)
	if !errors.Is(err, ErrSnapshotExpired) || !errors.Is(err, ErrNotFound) {
		t.Errorf("Snapshot(expired) = %v, want ErrSnapshotExpired matching ErrNotFound", err)
	}
	if _, err := ds.Read(t.Context(), snap.ID); !errors.Is(err, ErrSnapshotExpired) {
		t.Errorf("Read(expired) = %v, want ErrSnapshotExpired", err)
	}
}

func assertPresent(t *testing.T, ds Dataset, snap *DatasetSnapshot) {
	t.Helper()
	if _, err := ds.Read(t.Context(), snap.ID); err != nil {
		t.Errorf("Read(%s) = %v, want readable", snap.ID, err)
	}
}

func newDefaultTombstones(t *testing.T, ds Dataset) tombstones {
	t.Helper()
	d, ok := ds.(*dataset)
	if !ok {
		t.Fatalf("unexpected Dataset implementation %T", ds)
	}
	return d.tombstones()
}

// setPointer points the events dataset's latest pointer at id.
func setPointer(t *testing.T, store Store, id string) {
	t.Helper()
	const p = "datasets/events/latest"
	if err := store.Delete(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), p, strings.NewReader(id)); err != nil {
		t.Fatal(err)
	}
}

// putTombstone writes tomb at expired/<at>, as an earlier run would have.
func putTombstone(t *testing.T, store Store, at DatasetSnapshotID, tomb tombstone) {
	t.Helper()
	data, err := json.Marshal(tomb)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), "datasets/events/expired/"+string(at), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// foreignTombstone records snapshot id as an earlier snapshot with that ID
// would have: files, committed at an instant no test snapshot shares.
func foreignTombstone(id DatasetSnapshotID, files []FileRef) tombstone {
	return newTombstone(id, &Manifest{CreatedAt: time.Unix(0, 1).UTC(), Files: files})
}

// vanishOnList deletes target right after any listing returns it, as a
// concurrent expiry would.
func vanishOnList(t *testing.T, hs *hookStore, target string) {
	t.Helper()
	hs.onList = func(_ string, keys []string) {
		if slices.Contains(keys, target) {
			_ = hs.Store.Delete(t.Context(), target)
		}
	}
}

// rewriteManifest replaces the manifest at p, as only a test may.
func rewriteManifest(t *testing.T, store Store, p string, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// idAt is the snapshot ID a write beginning at t would get, with a zero
// nonce.
func idAt(t time.Time) DatasetSnapshotID {
	return DatasetSnapshotID(formatSnapshotID(t, [nonceBytes]byte{}))
}

// -----------------------------------------------------------------------------
// Expire: requests and selection
// -----------------------------------------------------------------------------

func TestExpire_Validation(t *testing.T) {
	tests := []struct {
		name string
		hive bool
		req  ExpireRequest
		want error
	}{
		{"no selector", true, ExpireRequest{DryRun: true}, ErrInvalidArgument},
		{"empty IDs is unset", true, ExpireRequest{IDs: []DatasetSnapshotID{}}, ErrInvalidArgument},
		{"leading slash", true, ExpireRequest{Partition: "/run_id=a"}, ErrInvalidPath},
		{"dot-dot segment", true, ExpireRequest{Partition: "run_id=a/../x"}, ErrInvalidPath},
		{"empty segment", true, ExpireRequest{Partition: "run_id=a//x"}, ErrInvalidPath},
		{"backslash", true, ExpireRequest{Partition: `run_id=a\x`}, ErrInvalidPath},
		{"malformed before unsupported", false, ExpireRequest{Partition: "../x"}, ErrInvalidPath},
		{"partition without partitions", false, ExpireRequest{Partition: "run_id=a"}, ErrPartitionPrefixNotSupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFaultStore(NewMemory())
			ds := newDefaultRetention(t, fs)
			if tt.hive {
				ds = newHiveRetention(t, fs)
			}
			report, err := ds.Expire(t.Context(), tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Expire = %v, want %v", err, tt.want)
			}
			if report == nil {
				t.Fatal("report is nil on error")
			}
			if calls := len(fs.ListCalls()) + len(fs.GetCalls()); calls != 0 {
				t.Errorf("store touched before validation: %d calls", calls)
			}
		})
	}
}

func TestExpire_Selection(t *testing.T) {
	t.Run("by ID removes manifest and data", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)
		third := writeRecord(t, ds, 3)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, first.ID)
		}
		if report.Objects != len(first.Manifest.Files) || report.Bytes != totalBytes(first.Manifest.Files) {
			t.Errorf("Objects/Bytes = %d/%d, want %d/%d", report.Objects, report.Bytes, len(first.Manifest.Files), totalBytes(first.Manifest.Files))
		}
		if report.RetainedHead != "" || len(report.Mixed) != 0 || len(report.Skipped) != 0 {
			t.Errorf("unexpected report fields: %+v", report)
		}

		assertGone(t, store, ds, first)
		assertPresent(t, ds, second)
		assertPresent(t, ds, third)

		snaps, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(snaps) != 2 {
			t.Errorf("Snapshots = %d, want 2", len(snaps))
		}
		latest, err := ds.Latest(t.Context())
		if err != nil || latest.ID != third.ID {
			t.Errorf("Latest = %v, %v; want %s", latest, err, third.ID)
		}

		ok, err := store.Exists(t.Context(), "datasets/events/expired/"+string(first.ID))
		if err != nil || !ok {
			t.Errorf("tombstone missing: %v %v", ok, err)
		}
	})

	t.Run("before uses the snapshot ID instant", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		old := writeRecord(t, ds, 1)
		cutoff := time.Now()
		newer := writeRecord(t, ds, 2)
		writeRecord(t, ds, 3) // head

		report, err := ds.Expire(t.Context(), ExpireRequest{Before: cutoff})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{old.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, old.ID)
		}
		assertPresent(t, ds, newer)
	})

	t.Run("selectors intersect", func(t *testing.T) {
		ds := newDefaultRetention(t, NewMemory())
		a := writeRecord(t, ds, 1)
		cutoff := time.Now()
		b := writeRecord(t, ds, 2)
		writeRecord(t, ds, 3)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{a.ID, b.ID}, Before: cutoff, DryRun: true})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{a.ID}) {
			t.Errorf("Expired = %v, want only [%s]", report.Expired, a.ID)
		}
	})

	t.Run("nothing selected is not an error", func(t *testing.T) {
		ds := newDefaultRetention(t, NewMemory())
		writeRecord(t, ds, 1)
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{"404"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Expired)+len(report.Mixed) != 0 || report.RetainedHead != "" {
			t.Errorf("report = %+v, want empty", report)
		}
	})

	t.Run("flat layout", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store, WithLayout(NewFlatLayout()))
		first := writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)

		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err != nil {
			t.Fatal(err)
		}
		assertGone(t, store, ds, first)
		snaps, err := ds.Snapshots(t.Context())
		if err != nil || len(snaps) != 1 || snaps[0].ID != second.ID {
			t.Errorf("Snapshots = %v, %v; want only %s (the tombstone is not a snapshot)", snaps, err, second.ID)
		}
	})
}

func TestExpire_Partition(t *testing.T) {
	t.Run("matches whole segments", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		run1 := writeRun(t, ds, "run-1", "start", "end")
		run10 := writeRun(t, ds, "run-10", "start")
		writeRun(t, ds, "run-2", "start") // head

		report, err := ds.Expire(t.Context(), ExpireRequest{Partition: "run_id=run-1"})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{run1.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, run1.ID)
		}
		if report.Objects != 2 || len(report.Mixed) != 0 {
			t.Errorf("Objects = %d, Mixed = %v; want 2 (one per event type) and none mixed", report.Objects, report.Mixed)
		}
		assertGone(t, store, ds, run1)
		assertPresent(t, ds, run10)
	})

	t.Run("a trailing slash is valid", func(t *testing.T) {
		ds := newHiveRetention(t, NewMemory())
		writeRun(t, ds, "a", "x")
		writeRun(t, ds, "b", "x")
		report, err := ds.Expire(t.Context(), ExpireRequest{Partition: "run_id=a/"})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Expired) != 1 {
			t.Errorf("Expired = %v, want one snapshot", report.Expired)
		}
	})

	t.Run("mixed snapshot is kept", func(t *testing.T) {
		ds := newHiveRetention(t, NewMemory())
		mixed := writeRun(t, ds, "a", "x")
		// Records from two runs in one write make one snapshot spanning both.
		both, err := ds.Write(t.Context(), R(D{"run_id": "a", "event_type": "x"}, D{"run_id": "b", "event_type": "x"}), Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		writeRun(t, ds, "c", "x") // head

		report, err := ds.Expire(t.Context(), ExpireRequest{Partition: "run_id=a"})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Mixed, []DatasetSnapshotID{both.ID}) {
			t.Errorf("Mixed = %v, want [%s]", report.Mixed, both.ID)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{mixed.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, mixed.ID)
		}
		assertPresent(t, ds, both)

		// Named by ID without Partition, the mixed snapshot can be expired.
		report, err = ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{both.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{both.ID}) {
			t.Errorf("Expired by ID = %v, want [%s]", report.Expired, both.ID)
		}
	})
}

func TestExpire_DryRun(t *testing.T) {
	t.Run("changes nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		a := writeRun(t, ds, "a", "x", "y")
		head := writeRun(t, ds, "b", "x")
		before := storeKeys(t, store)

		report, err := ds.Expire(t.Context(), ExpireRequest{Before: time.Now().Add(time.Hour), DryRun: true})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{a.ID}) || report.RetainedHead != head.ID {
			t.Errorf("report = %+v, want Expired [%s] RetainedHead %s", report, a.ID, head.ID)
		}
		if report.Objects != 2 || report.Bytes != totalBytes(a.Manifest.Files) {
			t.Errorf("Objects/Bytes = %d/%d, want 2/%d", report.Objects, report.Bytes, totalBytes(a.Manifest.Files))
		}
		if after := storeKeys(t, store); !slices.Equal(before, after) {
			t.Errorf("dry run changed the store:\nbefore %v\nafter  %v", before, after)
		}
	})

	t.Run("fails where a real run would", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		putTombstone(t, mem, first.ID, newTombstone("123", first.Manifest))

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: true})
		if err == nil || len(report.Expired) != 0 {
			t.Errorf("dry run = %+v, %v; want the tombstone mismatch a real run hits", report, err)
		}
	})
}

// -----------------------------------------------------------------------------
// Expire: the head
// -----------------------------------------------------------------------------

func TestExpire_Head(t *testing.T) {
	t.Run("retained and reported", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)

		report, err := ds.Expire(t.Context(), ExpireRequest{Before: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		if report.RetainedHead != head.ID {
			t.Errorf("RetainedHead = %q, want %q", report.RetainedHead, head.ID)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, first.ID)
		}
		assertPresent(t, ds, head)
		if ok, _ := store.Exists(t.Context(), "datasets/events/expired/"+string(head.ID)); ok {
			t.Error("a tombstone was written for the retained head")
		}
	})

	t.Run("superseded mid-run stays retained", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		first := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)

		// After the first snapshot is gone, another writer commits: the head
		// the run saw at preflight is no longer the head when its turn comes.
		writer := newDefaultRetention(t, mem)
		hs.onDelete = func(p string) error {
			if strings.Contains(p, string(first.ID)+"/data/") {
				hs.onDelete = nil
				writeRecord(t, writer, 3)
			}
			return nil
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID, head.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if report.RetainedHead != head.ID || !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("report = %+v, want %s retained and only %s expired", report, head.ID, first.ID)
		}
		assertPresent(t, ds, head)
	})

	t.Run("uncommitted expires nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		setPointer(t, store, "9999999999999999999")
		before := storeKeys(t, store)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}})
		if !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("Expire = %v, want ErrHeadUncommitted", err)
		}
		if len(report.Expired) != 0 {
			t.Errorf("Expired = %v, want none", report.Expired)
		}
		if after := storeKeys(t, store); !slices.Equal(before, after) {
			t.Error("store changed although the head was uncommitted")
		}
	})

	t.Run("uncommitted mid-run reports completed work", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		first := writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)
		writeRecord(t, ds, 3)

		// Once the first snapshot's data is deleted, a commit starts: the
		// pointer moves to a snapshot without a manifest yet.
		hs.onDelete = func(p string) error {
			if strings.Contains(p, string(first.ID)+"/data/") {
				setPointer(t, mem, "9999999999999999999")
			}
			return nil
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID, second.ID}})
		if !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("Expire = %v, want ErrHeadUncommitted", err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("Expired = %v, want [%s] (work before the error)", report.Expired, first.ID)
		}
		assertPresent(t, ds, second)
	})

	t.Run("uncommitted before a retained head stops the run", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		first := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)

		// After the first snapshot is expired, a commit starts before the run
		// reaches the head it already protects.
		hs.onDelete = func(p string) error {
			if strings.Contains(p, string(first.ID)+"/data/") {
				setPointer(t, mem, "9999999999999999999")
			}
			return nil
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID, head.ID}})
		if !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("Expire = %v, want ErrHeadUncommitted", err)
		}
		if report.RetainedHead != "" {
			t.Errorf("RetainedHead = %q, want none: the run stopped before deciding it", report.RetainedHead)
		}
		assertPresent(t, ds, head)
	})

	t.Run("without a pointer the scanned head is protected", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)
		if err := store.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		// A fresh dataset handle has no in-process memory of the head.
		ds = newDefaultRetention(t, store)

		report, err := ds.Expire(t.Context(), ExpireRequest{Before: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		if report.RetainedHead != head.ID || !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("report = %+v, want head %s retained and %s expired", report, head.ID, first.ID)
		}
	})

	t.Run("without a pointer a head vanishing after the listing", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)
		third := writeRecord(t, ds, 3)
		if err := mem.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		ds = newDefaultRetention(t, casHookStore{hs})

		// The head scan's listing still shows the newest snapshot, whose
		// manifest is then deleted by a concurrent expiry.
		listings := 0
		hs.onList = func(string, []string) {
			if listings++; listings == 2 {
				_ = mem.Delete(t.Context(), "datasets/events/snapshots/"+string(third.ID)+"/manifest.json")
			}
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{Before: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		if report.RetainedHead != second.ID {
			t.Errorf("RetainedHead = %q, want %q (the newest snapshot still loadable)", report.RetainedHead, second.ID)
		}
		assertPresent(t, ds, second)
	})

	t.Run("legacy partition-only head is committed", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		first := writeRun(t, ds, "a", "x")
		head := writeRun(t, ds, "b", "x")
		// Data written before canonical manifests existed has only partition
		// copies, and the pointer may name such a snapshot after self-heal.
		if err := store.Delete(t.Context(), "datasets/events/segments/"+string(head.ID)+"/manifest.json"); err != nil {
			t.Fatal(err)
		}

		report, err := ds.Expire(t.Context(), ExpireRequest{Before: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatalf("Expire = %v, want the legacy head treated as committed", err)
		}
		if report.RetainedHead != head.ID || !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("report = %+v, want %s retained and %s expired", report, head.ID, first.ID)
		}
	})
}

// -----------------------------------------------------------------------------
// Expire: tombstones
// -----------------------------------------------------------------------------

func TestExpire_Tombstone(t *testing.T) {
	t.Run("records the inventory", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		m := *first.Manifest
		m.Files = append(m.Files, FileRef{Path: "elsewhere/x", SizeBytes: 7})
		rewriteManifest(t, store, "datasets/events/snapshots/"+string(first.ID)+"/manifest.json", &m)

		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err != nil {
			t.Fatal(err)
		}
		ts, err := newDefaultTombstones(t, ds).lookup(t.Context(), first.ID)
		if err != nil {
			t.Fatalf("lookup = %v, want the tombstone", err)
		}
		if !slices.Equal(ts.Files, m.Files) {
			t.Errorf("inventory = %v, want the manifest's Files[] %v", ts.Files, m.Files)
		}
	})

	t.Run("existing", func(t *testing.T) {
		stores := map[string]func(Store) Store{
			"cas":     func(s Store) Store { return casHookStore{&hookStore{Store: s}} },
			"non-cas": func(s Store) Store { return &hookStore{Store: s} },
		}
		for name, wrap := range stores {
			t.Run(name+"/same snapshot resumes", func(t *testing.T) {
				mem := NewMemory()
				ds := newDefaultRetention(t, wrap(mem))
				first := writeRecord(t, ds, 1)
				writeRecord(t, ds, 2)
				putTombstone(t, mem, first.ID, newTombstone(first.ID, first.Manifest))

				if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err != nil {
					t.Fatal(err)
				}
				assertGone(t, mem, ds, first)
			})
			t.Run(name+"/other snapshot fails", func(t *testing.T) {
				mem := NewMemory()
				ds := newDefaultRetention(t, wrap(mem))
				first := writeRecord(t, ds, 1)
				writeRecord(t, ds, 2)
				putTombstone(t, mem, first.ID, newTombstone("123", first.Manifest))

				report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}})
				if err == nil {
					t.Fatal("Expire succeeded over a tombstone recording another snapshot")
				}
				if len(report.Expired) != 0 {
					t.Errorf("Expired = %v, want none", report.Expired)
				}
				assertPresent(t, ds, first)
			})
		}
	})

	t.Run("zero length", func(t *testing.T) {
		stores := map[string]struct {
			wrap    func(Store) Store
			succeed bool // CAS from "" replaces a zero-length object; Put does not
		}{
			"cas":     {func(s Store) Store { return casHookStore{&hookStore{Store: s}} }, true},
			"non-cas": {func(s Store) Store { return &hookStore{Store: s} }, false},
		}
		for name, tt := range stores {
			for _, dryRun := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/dry=%v", name, dryRun), func(t *testing.T) {
					mem := NewMemory()
					ds := newDefaultRetention(t, tt.wrap(mem))
					first := writeRecord(t, ds, 1)
					writeRecord(t, ds, 2)
					if err := mem.Put(t.Context(), "datasets/events/expired/"+string(first.ID), strings.NewReader("")); err != nil {
						t.Fatal(err)
					}
					_, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: dryRun})
					if (err == nil) != tt.succeed {
						t.Errorf("Expire = %v, want success %v (dry run and real run agree)", err, tt.succeed)
					}
				})
			}
		}
	})

	t.Run("concurrent one is accepted", func(t *testing.T) {
		mem := NewMemory()
		rs := &racingStore{hookStore: &hookStore{Store: mem}}
		ds := newDefaultRetention(t, rs)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}})
		if err != nil || !rs.raced {
			t.Fatalf("Expire = %v (raced %v), want the concurrent tombstone accepted", err, rs.raced)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{first.ID}) {
			t.Errorf("Expired = %v, want [%s]", report.Expired, first.ID)
		}
		assertGone(t, mem, ds, first)
	})

	t.Run("concurrent one of another inventory is refused", func(t *testing.T) {
		mem := NewMemory()
		rs := &racingStore{hookStore: &hookStore{Store: mem}}
		ds := newDefaultRetention(t, rs)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		rival, err := json.Marshal(foreignTombstone(first.ID, []FileRef{{Path: "datasets/events/snapshots/" + string(first.ID) + "/data/old", SizeBytes: 1}}))
		if err != nil {
			t.Fatal(err)
		}
		rs.rival = string(rival)

		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err == nil || !rs.raced {
			t.Fatalf("Expire = %v (raced %v), want the rival tombstone refused", err, rs.raced)
		}
		assertPresent(t, ds, first)
	})

	t.Run("ID not in the generated form never reaches a path", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		writeRecord(t, ds, 1)
		victim := "victim"
		if err := mem.Put(t.Context(), victim, strings.NewReader(`{"snapshot_id":"../../../victim","files":[]}`)); err != nil {
			t.Fatal(err)
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{"../../../victim"}})
		if err != nil || len(report.Expired) != 0 {
			t.Errorf("Expire = %+v, %v; want nothing selected", report, err)
		}
		if ok, _ := mem.Exists(t.Context(), victim); !ok {
			t.Error("an object outside the dataset was deleted")
		}
	})
}

// TestExpire_InvalidManifest covers manifests that decode but break the
// manifest invariants: nothing is recorded or deleted from them.
func TestExpire_InvalidManifest(t *testing.T) {
	for name, corrupt := range map[string]func(*Manifest){
		"zero created at": func(m *Manifest) { m.CreatedAt = time.Time{} },
		"negative size":   func(m *Manifest) { m.Files[0].SizeBytes = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			mem := NewMemory()
			ds := newDefaultRetention(t, mem)
			first := writeRecord(t, ds, 1)
			writeRecord(t, ds, 2)
			m := *first.Manifest
			m.Files = slices.Clone(m.Files)
			corrupt(&m)
			rewriteManifest(t, mem, "datasets/events/snapshots/"+string(first.ID)+"/manifest.json", &m)

			if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err == nil {
				t.Fatal("Expire expired a snapshot from an invalid manifest")
			}
			if ok, _ := mem.Exists(t.Context(), "datasets/events/expired/"+string(first.ID)); ok {
				t.Error("a tombstone was recorded from an invalid manifest")
			}
		})
	}
}

// TestExpire_CorruptTombstone covers tombstones no manifest can stand in
// for: each fails the run rather than steering a delete.
func TestExpire_CorruptTombstone(t *testing.T) {
	t.Run("zero length with no manifest left", func(t *testing.T) {
		for _, dryRun := range []bool{true, false} {
			mem := NewMemory()
			ds := newDefaultRetention(t, casHookStore{&hookStore{Store: mem}})
			writeRecord(t, ds, 1)
			// Nothing is left to record a tombstone from, so the placeholder
			// cannot be replaced and is not an absent tombstone.
			gone := idAt(time.Now().Add(-time.Hour))
			if err := mem.Put(t.Context(), "datasets/events/expired/"+string(gone), strings.NewReader("")); err != nil {
				t.Fatal(err)
			}
			if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{gone}, DryRun: dryRun}); !errors.Is(err, errEmptyTombstone) {
				t.Errorf("Expire(dry=%v) = %v, want errEmptyTombstone", dryRun, err)
			}
		}
	})

	t.Run("no commit instant", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		writeRecord(t, ds, 1)
		gone := idAt(time.Now().Add(-time.Hour))
		body := `{"snapshot_id":"` + string(gone) + `","files":[]}`
		if err := mem.Put(t.Context(), "datasets/events/expired/"+string(gone), strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		_, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{gone}})
		if err == nil || !strings.Contains(err.Error(), "commit instant") {
			t.Errorf("Expire = %v, want the tombstone refused for its missing commit instant", err)
		}
	})

	t.Run("invalid inventory", func(t *testing.T) {
		for name, files := range map[string]string{
			"no inventory":  ``,
			"empty path":    `,"files":[{"path":"","size_bytes":1}]`,
			"negative size": `,"files":[{"path":"datasets/events/snapshots/x/data/f","size_bytes":-1}]`,
		} {
			t.Run(name, func(t *testing.T) {
				mem := NewMemory()
				ds := newDefaultRetention(t, mem)
				writeRecord(t, ds, 1)
				// No manifest is left to compare against: only validation stands
				// between this inventory and the deletes.
				gone := idAt(time.Now().Add(-time.Hour))
				body := `{"snapshot_id":"` + string(gone) + `","manifest_created_at":"2026-01-01T00:00:00Z"` + files + `}`
				if err := mem.Put(t.Context(), "datasets/events/expired/"+string(gone), strings.NewReader(body)); err != nil {
					t.Fatal(err)
				}
				for _, dryRun := range []bool{true, false} {
					_, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{gone}, DryRun: dryRun})
					if err == nil || !strings.Contains(err.Error(), "inventory") {
						t.Errorf("Expire(dry=%v) = %v, want the invalid inventory refused", dryRun, err)
					}
				}
			})
		}
	})
}

// A tombstone left by an earlier snapshot with the same ID never steers
// deletes of a later one.
func TestExpire_ReusedID(t *testing.T) {
	t.Run("same files with another commit instant is refused", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		// An earlier snapshot with the same ID listed the very same files;
		// only its commit instant tells it apart.
		earlier := *first.Manifest
		earlier.CreatedAt = earlier.CreatedAt.Add(-time.Hour)
		putTombstone(t, mem, first.ID, newTombstone(first.ID, &earlier))

		for _, dryRun := range []bool{true, false} {
			if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: dryRun}); err == nil {
				t.Errorf("Expire(dry=%v) used another snapshot's tombstone", dryRun)
			}
		}
		assertPresent(t, ds, first)
	})

	t.Run("tombstone of another inventory is refused", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		// An earlier snapshot with the same ID, expired long ago, left this.
		putTombstone(t, mem, first.ID, foreignTombstone(first.ID, []FileRef{{Path: "datasets/events/snapshots/" + string(first.ID) + "/data/old", SizeBytes: 1}}))

		for _, dryRun := range []bool{true, false} {
			if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: dryRun}); err == nil {
				t.Errorf("Expire(dry=%v) used a tombstone that does not match the manifest", dryRun)
			}
		}
		assertPresent(t, ds, first)
	})

	t.Run("recorded tombstone must match every manifest copy", func(t *testing.T) {
		mem := NewMemory()
		ds := newHiveRetention(t, mem)
		run := writeRun(t, ds, "a", "x", "y")
		writeRun(t, ds, "b", "x")
		// The recorded tombstone matches the canonical copy, but a partition copy
		// left by an earlier snapshot with the same ID lists other objects.
		putTombstone(t, mem, run.ID, newTombstone(run.ID, run.Manifest))
		stray := "datasets/events/partitions/run_id=a/event_type=y/segments/" + string(run.ID) + "/manifest.json"
		other := *run.Manifest
		other.Files = []FileRef{{Path: "datasets/events/partitions/run_id=a/event_type=y/segments/" + string(run.ID) + "/data/old", SizeBytes: 1}}
		rewriteManifest(t, mem, stray, &other)

		for _, dryRun := range []bool{true, false} {
			if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}, DryRun: dryRun}); err == nil {
				t.Errorf("Expire(dry=%v) trusted a tombstone that a manifest copy contradicts", dryRun)
			}
		}
		assertPresent(t, ds, run)
	})

	t.Run("copy reappearing at a derived path is checked", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, casHookStore{hs})
		writeRun(t, ds, "b", "x")
		reused := idAt(time.Now().Add(-time.Hour))
		dataDir := "datasets/events/partitions/run_id=a/event_type=x/segments/" + string(reused)
		putTombstone(t, mem, reused, foreignTombstone(reused, []FileRef{{Path: dataDir + "/data/old", SizeBytes: 1}}))

		// After selection saw only the tombstone, a snapshot reusing the ID
		// writes a partition copy where the old inventory's copy was.
		copyPath := dataDir + "/manifest.json"
		hs.onList = func(string, []string) {
			hs.onList = nil
			m := Manifest{SnapshotID: reused, Files: []FileRef{{Path: dataDir + "/data/new", SizeBytes: 2}}}
			rewriteManifest(t, mem, copyPath, &m)
		}
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{reused}}); err == nil {
			t.Fatal("Expire trusted an old tombstone against a newer partition copy")
		}
		if ok, _ := mem.Exists(t.Context(), copyPath); !ok {
			t.Error("the newer snapshot's partition copy was deleted")
		}
	})

	t.Run("manifest reappearing for a tombstone is checked", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		writeRecord(t, ds, 1)
		reused := idAt(time.Now().Add(-time.Hour))
		putTombstone(t, mem, reused, foreignTombstone(reused, []FileRef{{Path: "datasets/events/snapshots/" + string(reused) + "/data/old", SizeBytes: 1}}))

		// After selection sees only the tombstone, a new snapshot reuses the ID.
		manifest := "datasets/events/snapshots/" + string(reused) + "/manifest.json"
		hs.onList = func(string, []string) {
			hs.onList = nil
			m := Manifest{SnapshotID: reused, Files: []FileRef{{Path: "datasets/events/snapshots/" + string(reused) + "/data/new", SizeBytes: 2}}}
			rewriteManifest(t, mem, manifest, &m)
		}
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{reused}}); err == nil {
			t.Fatal("Expire used an old tombstone against a newer manifest")
		}
		if ok, _ := mem.Exists(t.Context(), manifest); !ok {
			t.Error("the newer snapshot's manifest was deleted")
		}
	})
}

func TestExpire_Resumption(t *testing.T) {
	t.Run("from a tombstone with no manifest left", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, casHookStore{hs})
		run := writeRun(t, ds, "a", "x", "y")
		writeRun(t, ds, "b", "x")

		// Every manifest is deleted, then the second data delete fails.
		injected := errors.New("injected")
		deletes := 0
		hs.onDelete = func(p string) error {
			if !strings.Contains(p, "/data/") {
				return nil
			}
			if deletes++; deletes == 2 {
				return injected
			}
			return nil
		}
		first, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}})
		if !errors.Is(err, injected) {
			t.Fatalf("Expire = %v, want the injected error", err)
		}
		hs.onDelete = nil

		// The same request selects the snapshot by its tombstone and finishes,
		// once, however often the ID is named.
		second, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID, run.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(second.Expired, []DatasetSnapshotID{run.ID}) {
			t.Errorf("rerun Expired = %v, want [%s]", second.Expired, run.ID)
		}
		// Each run counts only what it deleted; together, the whole snapshot.
		if first.Objects != 1 || second.Objects != 1 || first.Bytes+second.Bytes != totalBytes(run.Manifest.Files) {
			t.Errorf("Objects %d+%d, Bytes %d+%d; want 1+1 and %d bytes in total",
				first.Objects, second.Objects, first.Bytes, second.Bytes, totalBytes(run.Manifest.Files))
		}
		assertGone(t, mem, ds, run)

		// Once finished, the snapshot is no longer selected.
		third, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}})
		if err != nil || third.Objects != 0 {
			t.Errorf("third run = %+v, %v; want nothing left to count", third, err)
		}
	})

	t.Run("interrupted after the canonical delete is not resurrected", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, casHookStore{hs})
		run := writeRun(t, ds, "a", "x")
		writeRun(t, ds, "b", "x")

		failPartitionManifest := errors.New("injected")
		hs.onDelete = func(p string) error {
			if strings.Contains(p, "partitions/") && strings.HasSuffix(p, manifestFile) {
				return failPartitionManifest
			}
			return nil
		}
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}})
		if !errors.Is(err, failPartitionManifest) {
			t.Fatalf("Expire = %v, want the injected error", err)
		}
		if !slices.Equal(report.Expired, []DatasetSnapshotID{run.ID}) {
			t.Errorf("Expired = %v: the canonical manifest is gone, so the snapshot is expired", report.Expired)
		}
		// A partition manifest survives, but Snapshot must not find it by scan.
		if _, err := ds.Snapshot(t.Context(), run.ID); !errors.Is(err, ErrSnapshotExpired) {
			t.Errorf("Snapshot = %v, want ErrSnapshotExpired", err)
		}

		// Rerunning the same request completes the expiry.
		hs.onDelete = nil
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}}); err != nil {
			t.Fatal(err)
		}
		assertGone(t, mem, ds, run)
	})
}

func TestExpire_PathSafety(t *testing.T) {
	t.Run("traversal in a FileRef deletes nothing outside", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		run := writeRun(t, ds, "a", "x")
		writeRun(t, ds, "b", "x")

		victim := "datasets/other/partitions/x/segments/" + string(run.ID) + "/manifest.json"
		if err := store.Put(t.Context(), victim, strings.NewReader("{}")); err != nil {
			t.Fatal(err)
		}
		m := *run.Manifest
		escape := "datasets/events/partitions/run_id=a/../../../other/partitions/x/segments/" + string(run.ID) + "/data/f"
		m.Files = append(m.Files, FileRef{Path: escape})
		rewriteManifest(t, store, "datasets/events/segments/"+string(run.ID)+"/manifest.json", &m)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(report.Skipped, escape) {
			t.Errorf("Skipped = %v, want the traversal path", report.Skipped)
		}
		if ok, _ := store.Exists(t.Context(), victim); !ok {
			t.Error("a manifest outside the dataset was deleted")
		}
	})

	t.Run("foreign FileRef is skipped", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)

		foreign := []string{
			"datasets/other/snapshots/" + string(first.ID) + "/data/x.jsonl", // another dataset
			"datasets/events/snapshots/123/data/x.jsonl",                     // another snapshot
			"datasets/events/snapshots/" + string(first.ID) + "/data/../x",   // not canonical
			"elsewhere/x.jsonl", // not a data path
			"datasets/events/snapshots/" + string(first.ID) + `/data/x\y`, // no store accepts it
		}
		stored := foreign[:len(foreign)-1] // the last is not a storable key
		m := *first.Manifest
		for _, p := range foreign {
			m.Files = append(m.Files, FileRef{Path: p, SizeBytes: 1})
		}
		for _, p := range stored {
			if err := store.Put(t.Context(), p, strings.NewReader("keep")); err != nil {
				t.Fatal(err)
			}
		}
		rewriteManifest(t, store, "datasets/events/snapshots/"+string(first.ID)+"/manifest.json", &m)

		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Skipped, foreign) {
			t.Errorf("Skipped = %v, want %v", report.Skipped, foreign)
		}
		if report.Objects != len(first.Manifest.Files) {
			t.Errorf("Objects = %d, want %d (foreign paths are not counted)", report.Objects, len(first.Manifest.Files))
		}
		for _, p := range stored {
			if ok, _ := store.Exists(t.Context(), p); !ok {
				t.Errorf("foreign object deleted: %s", p)
			}
		}
	})
}

func TestSnapshot_IDNotInGeneratedForm_IsNotExpired(t *testing.T) {
	ds := newDefaultRetention(t, NewMemory())
	writeRecord(t, ds, 1)
	// "../latest" would resolve to the pointer if it reached a store path.
	_, err := ds.Snapshot(t.Context(), "../latest")
	if errors.Is(err, ErrSnapshotExpired) || !errors.Is(err, ErrNotFound) {
		t.Errorf("Snapshot(../latest) = %v, want ErrNotFound only", err)
	}
}

// -----------------------------------------------------------------------------
// Enumeration tolerates deletion
// -----------------------------------------------------------------------------

func TestEnumeration_ToleratesDeletion(t *testing.T) {
	t.Run("listings skip a manifest deleted after listing", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, casHookStore{hs})
		gone := writeRun(t, ds, "a", "x")
		kept := writeRun(t, ds, "b", "x")
		reader, err := NewDatasetReader(NewMemoryFactoryFrom(hs), WithHiveLayout("run_id", "event_type"))
		if err != nil {
			t.Fatal(err)
		}

		paths := []string{
			"datasets/events/segments/" + string(gone.ID) + "/manifest.json",
			"datasets/events/partitions/run_id=a/event_type=x/segments/" + string(gone.ID) + "/manifest.json",
		}
		restore := func() {
			for _, p := range paths {
				rewriteManifest(t, mem, p, gone.Manifest)
			}
		}
		vanish := func() {
			restore()
			hs.onList = func(_ string, keys []string) {
				for _, p := range paths {
					if slices.Contains(keys, p) {
						_ = mem.Delete(t.Context(), p)
					}
				}
			}
		}

		vanish()
		snaps, err := ds.Snapshots(t.Context())
		if err != nil || len(snaps) != 1 || snaps[0].ID != kept.ID {
			t.Errorf("Snapshots = %v, %v; want only %s", snaps, err, kept.ID)
		}

		vanish()
		refs, err := reader.ListManifests(t.Context(), "events", "", ManifestListOptions{})
		if err != nil || len(refs) != 1 || refs[0].ID != kept.ID {
			t.Errorf("ListManifests = %v, %v; want only %s", refs, err, kept.ID)
		}

		vanish()
		parts, err := reader.ListPartitions(t.Context(), "events", PartitionListOptions{})
		if err != nil || len(parts) != 1 || parts[0].Path != "run_id=b/event_type=x" {
			t.Errorf("ListPartitions = %v, %v; want only run_id=b", parts, err)
		}
	})

	t.Run("latest scan skips a manifest deleted after listing", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		older := writeRecord(t, ds, 1)
		newest := writeRecord(t, ds, 2)
		if err := mem.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		vanishOnList(t, hs, "datasets/events/snapshots/"+string(newest.ID)+"/manifest.json")

		ds = newDefaultRetention(t, casHookStore{hs})
		latest, err := ds.Latest(t.Context())
		if err != nil || latest.ID != older.ID {
			t.Errorf("Latest = %v, %v; want %s (the newest vanished mid-scan)", latest, err, older.ID)
		}

		vanishOnList(t, hs, "datasets/events/snapshots/"+string(older.ID)+"/manifest.json")
		if err := mem.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		ds = newDefaultRetention(t, casHookStore{hs})
		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("Latest = %v, want ErrNoSnapshots when every manifest vanished", err)
		}
	})
}
