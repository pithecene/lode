package lode

import (
	"bytes"
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

// -----------------------------------------------------------------------------
// Validation
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

func TestExpire_TrailingSlashPartitionIsValid(t *testing.T) {
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
}

// -----------------------------------------------------------------------------
// Expiry
// -----------------------------------------------------------------------------

func TestExpire_ByID_RemovesManifestAndData(t *testing.T) {
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
}

func TestExpire_HeadIsRetainedAndReported(t *testing.T) {
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
}

func TestExpire_Before_UsesSnapshotIDInstant(t *testing.T) {
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
}

func TestExpire_IntersectsSelectors(t *testing.T) {
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
}

func TestExpire_NothingSelected_IsNotAnError(t *testing.T) {
	ds := newDefaultRetention(t, NewMemory())
	writeRecord(t, ds, 1)
	report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{"404"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expired)+len(report.Mixed) != 0 || report.RetainedHead != "" {
		t.Errorf("report = %+v, want empty", report)
	}
}

func TestExpire_Partition_MatchesWholeSegments(t *testing.T) {
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
}

func TestExpire_Partition_MixedSnapshotIsKept(t *testing.T) {
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
}

func TestExpire_DryRun_ChangesNothing(t *testing.T) {
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
}

func TestExpire_FlatLayout(t *testing.T) {
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
}

// -----------------------------------------------------------------------------
// The head
// -----------------------------------------------------------------------------

func TestExpire_UncommittedHead_ExpiresNothing(t *testing.T) {
	store := NewMemory()
	ds := newDefaultRetention(t, store)
	first := writeRecord(t, ds, 1)
	writeRecord(t, ds, 2)
	setPointer(t, store, "datasets/events/latest", "9999999999999999999")
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
}

func TestExpire_UncommittedHeadMidRun_ReportsCompletedWork(t *testing.T) {
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
			setPointer(t, mem, "datasets/events/latest", "9999999999999999999")
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
}

func TestExpire_NoPointer_ProtectsScannedHead(t *testing.T) {
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
}

func TestExpire_HeadSupersededMidRun_StaysRetained(t *testing.T) {
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
}

func TestExpire_DryRun_FailsWhereARealRunWould(t *testing.T) {
	mem := NewMemory()
	ds := newDefaultRetention(t, mem)
	first := writeRecord(t, ds, 1)
	writeRecord(t, ds, 2)
	putTombstone(t, mem, first.ID, "123", first.Manifest.Files)

	report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: true})
	if err == nil || len(report.Expired) != 0 {
		t.Errorf("dry run = %+v, %v; want the tombstone mismatch a real run hits", report, err)
	}
}

func TestExpire_ZeroLengthTombstone(t *testing.T) {
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
}

func TestExpire_TombstoneRecordsTheInventory(t *testing.T) {
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
	ts, found, err := newDefaultTombstones(t, ds).lookup(t.Context(), first.ID)
	if err != nil || !found {
		t.Fatalf("lookup = %v, %v; want the tombstone", found, err)
	}
	if !slices.Equal(ts.Files, m.Files) {
		t.Errorf("inventory = %v, want the manifest's Files[] %v", ts.Files, m.Files)
	}
}

func TestExpire_ResumesFromTombstoneWithNoManifestLeft(t *testing.T) {
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
}

func TestExpire_IDNotInGeneratedForm_NeverReachesAPath(t *testing.T) {
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
}

func TestExpire_TombstoneOfAnotherInventory_IsRefused(t *testing.T) {
	mem := NewMemory()
	ds := newDefaultRetention(t, mem)
	first := writeRecord(t, ds, 1)
	writeRecord(t, ds, 2)
	// An earlier snapshot with the same ID, expired long ago, left this.
	putTombstone(t, mem, first.ID, first.ID, []FileRef{{Path: "datasets/events/snapshots/" + string(first.ID) + "/data/old", SizeBytes: 1}})

	for _, dryRun := range []bool{true, false} {
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}, DryRun: dryRun}); err == nil {
			t.Errorf("Expire(dry=%v) used a tombstone that does not match the manifest", dryRun)
		}
	}
	assertPresent(t, ds, first)
}

// racingStore records a tombstone for the same snapshot just before the
// first tombstone CAS, as a concurrent expiry of the same ID would.
type racingStore struct {
	*hookStore
	raced bool
}

func (r *racingStore) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	cw := r.Store.(ConditionalWriter)
	if !r.raced && strings.Contains(p, "/expired/") {
		r.raced = true
		if err := cw.CompareAndSwap(ctx, p, "", replacement); err != nil {
			return err
		}
	}
	return cw.CompareAndSwap(ctx, p, expected, replacement)
}

func TestExpire_ConcurrentTombstone_IsAccepted(t *testing.T) {
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
}

func TestExpire_TombstoneWithoutInventory_IsCorrupt(t *testing.T) {
	mem := NewMemory()
	ds := newDefaultRetention(t, mem)
	first := writeRecord(t, ds, 1)
	writeRecord(t, ds, 2)
	body := `{"snapshot_id":"` + string(first.ID) + `"}`
	if err := mem.Put(t.Context(), "datasets/events/expired/"+string(first.ID), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err == nil {
		t.Fatal("Expire accepted a tombstone with no inventory")
	}
	assertPresent(t, ds, first)
}

func TestExpire_TraversalInFileRef_DeletesNothingOutside(t *testing.T) {
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
}

func TestExpire_LegacyPartitionOnlyHead_IsCommitted(t *testing.T) {
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
}

func newDefaultTombstones(t *testing.T, ds Dataset) tombstones {
	t.Helper()
	d, ok := ds.(*dataset)
	if !ok {
		t.Fatalf("unexpected Dataset implementation %T", ds)
	}
	return d.tombstones()
}

func setPointer(t *testing.T, store Store, p, id string) {
	t.Helper()
	if err := store.Delete(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), p, strings.NewReader(id)); err != nil {
		t.Fatal(err)
	}
}

// -----------------------------------------------------------------------------
// Tombstones and resumption
// -----------------------------------------------------------------------------

func TestExpire_ExistingTombstone(t *testing.T) {
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
			putTombstone(t, mem, first.ID, first.ID, first.Manifest.Files)

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
			putTombstone(t, mem, first.ID, "123", first.Manifest.Files)

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
}

// putTombstone writes a tombstone at expired/<at> that records snapshot
// records with the given inventory, as an earlier run would have.
func putTombstone(t *testing.T, store Store, at, records DatasetSnapshotID, files []FileRef) {
	t.Helper()
	data, err := json.Marshal(newTombstone(records, files))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), "datasets/events/expired/"+string(at), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func TestExpire_InterruptedAfterCanonicalDelete_IsNotResurrected(t *testing.T) {
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
}

func TestExpire_ForeignFileRef_IsSkipped(t *testing.T) {
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
}

// -----------------------------------------------------------------------------
// Enumeration tolerates deletion
// -----------------------------------------------------------------------------

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

func TestEnumeration_SkipsManifestDeletedAfterListing(t *testing.T) {
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
}

func TestLatestScan_SkipsManifestDeletedAfterListing(t *testing.T) {
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

func TestSnapshotInstant_OnlyGeneratedForm(t *testing.T) {
	for id, want := range map[DatasetSnapshotID]bool{
		"1792459022524750970": true,
		"0":                   true,
		"00123":               false,
		"+123":                false,
		"-1":                  false,
		"abc":                 false,
		"":                    false,
	} {
		if _, ok := snapshotInstant(id); ok != want {
			t.Errorf("snapshotInstant(%q) ok = %v, want %v", id, ok, want)
		}
	}
}
