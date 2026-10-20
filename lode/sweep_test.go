package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func putObject(t *testing.T, store Store, p string) {
	t.Helper()
	if err := store.Put(t.Context(), p, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, store Store, p string) bool {
	t.Helper()
	ok, err := store.Exists(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

const sweepGrace = 24 * time.Hour

// listInjector adds raw keys to listings, as a store that does not
// normalize its keys might return, hides keys from them, as a lagging
// listing would, fails one prefix's listing, and records deletes.
type listInjector struct {
	Store
	extra      []string
	hidden     []string
	deleted    []string
	failPrefix string // a listing of exactly this prefix fails
}

var errListFailed = errors.New("list failed")

func (l *listInjector) List(ctx context.Context, prefix string) ([]string, error) {
	if l.failPrefix != "" && prefix == l.failPrefix {
		return nil, errListFailed
	}
	keys, err := l.Store.List(ctx, prefix)
	keys = slices.DeleteFunc(keys, func(k string) bool { return slices.Contains(l.hidden, k) })
	for _, k := range l.extra {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, err
}

func (l *listInjector) Delete(ctx context.Context, p string) error {
	l.deleted = append(l.deleted, p)
	return l.Store.Delete(ctx, p)
}

func (l *listInjector) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	return l.Store.(ConditionalWriter).CompareAndSwap(ctx, p, expected, replacement)
}

// -----------------------------------------------------------------------------
// Requests
// -----------------------------------------------------------------------------

func TestSweep_Request(t *testing.T) {
	t.Run("non-positive grace is rejected", func(t *testing.T) {
		for _, grace := range []time.Duration{0, -time.Second} {
			fs := newFaultStore(NewMemory())
			ds := newDefaultRetention(t, fs)
			report, err := ds.Sweep(t.Context(), SweepRequest{Grace: grace})
			if !errors.Is(err, ErrInvalidArgument) || report == nil {
				t.Errorf("Sweep(grace=%s) = %v, %v; want ErrInvalidArgument and a report", grace, report, err)
			}
			if n := len(fs.ListCalls()); n != 0 {
				t.Errorf("Sweep(grace=%s) listed %d times before validating", grace, n)
			}
		}
	})

	t.Run("empty dataset", func(t *testing.T) {
		ds := newDefaultRetention(t, NewMemory())
		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil || report.Objects+len(report.Settled)+len(report.Unparseable) != 0 {
			t.Errorf("Sweep = %+v, %v; want an empty report", report, err)
		}
	})

	t.Run("dry run reports the real run", func(t *testing.T) {
		setup := func(t *testing.T) (Store, Dataset) {
			store := NewMemory()
			ds := newDefaultRetention(t, store)
			first := writeRecord(t, ds, 1)
			writeRecord(t, ds, 2)
			putTombstone(t, store, first.ID, newTombstone(first.ID, first.Manifest))
			putObject(t, store, "datasets/events/snapshots/"+string(idAt(time.Now().Add(-48*time.Hour)))+"/data/x")
			return store, ds
		}

		store, ds := setup(t)
		before := storeKeys(t, store)
		dry, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace, DryRun: true})
		if err != nil {
			t.Fatal(err)
		}
		if after := storeKeys(t, store); !slices.Equal(before, after) {
			t.Error("dry run changed the store")
		}

		_, ds = setup(t)
		wet, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		// Separate setups mint different IDs; everything else must agree.
		shape := func(r *SweepReport) string {
			return fmt.Sprint(len(r.Settled), r.Objects, r.Bytes, r.Unsized, r.Orphans, r.Unparseable)
		}
		if shape(dry) != shape(wet) {
			t.Errorf("dry run %+v != real run %+v", dry, wet)
		}
		if wet.Objects != 2 || wet.Orphans != 1 || len(wet.Settled) != 1 {
			t.Errorf("real run = %+v, want one settled snapshot and one orphan", wet)
		}
	})

	t.Run("a listing failure still reports invalid tombstones", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newDefaultRetention(t, lister)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		tomb := "datasets/events/expired/" + string(first.ID)
		if err := mem.Put(t.Context(), tomb, strings.NewReader("{}")); err != nil {
			t.Fatal(err)
		}
		lister.failPrefix = "datasets/events/snapshots/"

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if !errors.Is(err, errListFailed) {
			t.Fatalf("Sweep = %v, want the listing failure", err)
		}
		if !slices.Equal(report.Unparseable, []string{tomb}) {
			t.Errorf("Unparseable = %v, want the invalid tombstone examined before the failure", report.Unparseable)
		}
	})

	t.Run("a delete failure reports completed work", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		writeRecord(t, ds, 1)
		old := time.Now().Add(-48 * time.Hour)
		first := "datasets/events/snapshots/" + string(idAt(old)) + "/data/a"
		second := "datasets/events/snapshots/" + string(idAt(old.Add(time.Second))) + "/data/b"
		putObject(t, mem, first)
		putObject(t, mem, second)

		injected := errors.New("injected")
		hs.onDelete = func(p string) error {
			if p == second {
				return injected
			}
			return nil
		}
		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if !errors.Is(err, injected) {
			t.Fatalf("Sweep = %v, want the injected error", err)
		}
		if report.Objects != 1 || report.Orphans != 1 {
			t.Errorf("report = %+v, want the one orphan deleted before the failure", report)
		}
	})
}

// -----------------------------------------------------------------------------
// Settling interrupted expiries
// -----------------------------------------------------------------------------

func TestSweep_Settle(t *testing.T) {
	t.Run("a manifest the listing omits is still removed", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newDefaultRetention(t, lister)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		putTombstone(t, mem, first.ID, newTombstone(first.ID, first.Manifest))
		manifest := "datasets/events/snapshots/" + string(first.ID) + "/manifest.json"
		lister.hidden = []string{manifest}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Settled, []DatasetSnapshotID{first.ID}) || exists(t, mem, manifest) {
			t.Errorf("report = %+v, manifest present %v; want the hidden manifest removed and the snapshot settled", report, exists(t, mem, manifest))
		}
	})

	t.Run("an interrupted expiry", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, casHookStore{hs})
		run := writeRun(t, ds, "a", "x", "y")
		writeRun(t, ds, "b", "x")
		stray := "datasets/events/partitions/run_id=a/event_type=x/segments/" + string(run.ID) + "/data/partial.bin"
		putObject(t, mem, stray)

		hs.onDelete = func(p string) error {
			if strings.Contains(p, "partitions/") && strings.HasSuffix(p, manifestFile) {
				return errors.New("injected")
			}
			return nil
		}
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{run.ID}}); err == nil {
			t.Fatal("Expire succeeded despite the injected failure")
		}
		hs.onDelete = nil

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: 1000 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Settled, []DatasetSnapshotID{run.ID}) {
			t.Errorf("Settled = %v, want [%s]", report.Settled, run.ID)
		}
		// Two recorded data files plus the stray the inventory never listed, all
		// regardless of grace: the recorded ones are sized, the stray is not.
		if report.Objects != 3 || report.Bytes != totalBytes(run.Manifest.Files) || report.Unsized != 1 || report.Orphans != 0 {
			t.Errorf("report = %+v, want 3 objects, %d bytes, 1 unsized, 0 orphans", report, totalBytes(run.Manifest.Files))
		}
		assertGone(t, mem, ds, run)
	})

	t.Run("an expiry with no manifest left", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, casHookStore{hs})
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		hs.onDelete = func(p string) error {
			if strings.Contains(p, "/data/") {
				return errors.New("injected")
			}
			return nil
		}
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err == nil {
			t.Fatal("Expire succeeded despite the injected failure")
		}
		hs.onDelete = nil

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: 1000 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		// Expire already removed every manifest, so nothing is settled this
		// run; the tombstone still marks the data as reclaimable.
		if len(report.Settled) != 0 || report.Objects != len(first.Manifest.Files) ||
			report.Bytes != totalBytes(first.Manifest.Files) || report.Unsized != 0 {
			t.Errorf("report = %+v, want %s's data reclaimed with its recorded sizes", report, first.ID)
		}
		assertGone(t, mem, ds, first)
	})
}

// -----------------------------------------------------------------------------
// Reclaiming objects
// -----------------------------------------------------------------------------

func TestSweep_Reclaim(t *testing.T) {
	t.Run("an orphan whose manifest the listing omits is kept", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		writeRecord(t, ds, 1)
		old := idAt(time.Now().Add(-48 * time.Hour))
		data := "datasets/events/snapshots/" + string(old) + "/data/x"
		manifest := "datasets/events/snapshots/" + string(old) + "/manifest.json"
		putObject(t, mem, data)
		putObject(t, mem, manifest)
		ds = newDefaultRetention(t, &listInjector{Store: mem, hidden: []string{manifest}})

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if report.Objects != 0 || !exists(t, mem, data) {
			t.Errorf("report = %+v, want the data a present manifest describes kept", report)
		}
	})

	t.Run("only orphans past grace", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		live := writeRun(t, ds, "a", "x")

		old := "datasets/events/partitions/run_id=a/event_type=x/segments/" + string(idAt(time.Now().Add(-48*time.Hour))) + "/data/part.jsonl"
		young := "datasets/events/partitions/run_id=a/event_type=x/segments/" + string(idAt(time.Now())) + "/data/part.jsonl"
		noInstant := "datasets/events/partitions/run_id=a/event_type=x/segments/abc/data/part.jsonl"
		sidecar := "datasets/events/partitions/run_id=a/files/run.json"
		for _, p := range []string{old, young, noInstant, sidecar} {
			putObject(t, store, p)
		}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if report.Objects != 1 || report.Orphans != 1 || report.Unsized != 1 || report.Bytes != 0 || len(report.Settled) != 0 {
			t.Errorf("report = %+v, want one orphan reclaimed", report)
		}
		if !slices.Equal(report.Unparseable, []string{noInstant}) {
			t.Errorf("Unparseable = %v, want [%s] (the sidecar is out of scope)", report.Unparseable, noInstant)
		}
		want := map[string]bool{old: false, young: true, noInstant: true, sidecar: true}
		for p, kept := range want {
			if exists(t, store, p) != kept {
				t.Errorf("%s present = %v, want %v", p, !kept, kept)
			}
		}
		assertPresent(t, ds, live)
	})

	t.Run("a live snapshot keeps unlisted objects", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		live := writeRecord(t, ds, 1)
		// An object in a live snapshot's data prefix that Files[] does not list
		// (a caller inventory kept in Metadata) is kept: reclaim is per snapshot.
		stray := "datasets/events/snapshots/" + string(live.ID) + "/data/extra.bin"
		putObject(t, store, stray)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Nanosecond})
		if err != nil {
			t.Fatal(err)
		}
		if report.Objects != 0 || !exists(t, store, stray) {
			t.Errorf("report = %+v, stray present = %v; want nothing reclaimed", report, exists(t, store, stray))
		}
	})

	t.Run("a crashed commit's data is an orphan", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		writeRecord(t, ds, 1)
		// A commit advanced the pointer, then crashed before its manifest.
		crashed := idAt(time.Now().Add(-48 * time.Hour))
		data := "datasets/events/snapshots/" + string(crashed) + "/data/x"
		putObject(t, store, data)
		setPointer(t, store, string(crashed))

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if report.Orphans != 1 || exists(t, store, data) {
			t.Errorf("report = %+v, want the crashed commit's data reclaimed after grace", report)
		}
	})

	t.Run("non-canonical keys are out of scope", func(t *testing.T) {
		mem := NewMemory()
		listed := []string{
			"datasets/events/expired/1\\2", // no snapshot ID, and no store accepts it
			"datasets/events/snapshots/" + string(idAt(time.Now().Add(-48*time.Hour))) + "/data/../../../other/x", // cleans elsewhere
		}
		lister := &listInjector{Store: mem, extra: listed}
		ds := newDefaultRetention(t, lister)
		writeRecord(t, ds, 1)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatalf("Sweep = %v, want invalid keys handled, not fatal", err)
		}
		if len(report.Unparseable) != 0 || report.Objects != 0 {
			t.Errorf("report = %+v, want both keys out of scope: not reported, not deleted", report)
		}
		if n := len(lister.deleted); n != 0 {
			t.Errorf("deleted %v, want nothing", lister.deleted)
		}
	})

	t.Run("a repeated filesystem run is quiet", func(t *testing.T) {
		dir := t.TempDir()
		ds, err := NewDataset("events", NewFSFactory(dir), WithCodec(NewJSONLCodec()))
		if err != nil {
			t.Fatal(err)
		}
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		if _, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{first.ID}}); err != nil {
			t.Fatal(err)
		}
		// The filesystem store's CAS leaves a .lock companion next to the
		// tombstone; it is not a tombstone, and a finished expiry is not news.
		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled)+report.Objects+len(report.Unparseable) != 0 {
			t.Errorf("report = %+v, want nothing to do", report)
		}
	})
}

// -----------------------------------------------------------------------------
// The head
// -----------------------------------------------------------------------------

func TestSweep_Head(t *testing.T) {
	t.Run("is never settled", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		head := writeRecord(t, ds, 1)
		putTombstone(t, store, head.ID, newTombstone(head.ID, head.Manifest)) // misplaced: Lode never tombstones a head

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Nanosecond})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || report.Objects != 0 {
			t.Errorf("report = %+v, want the head untouched", report)
		}
		assertPresent(t, ds, head)
	})

	t.Run("without a pointer the scanned head is held", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)
		legacyize(t, store, first, head)
		putTombstone(t, store, head.ID, newTombstone(head.ID, head.Manifest)) // misplaced

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Nanosecond})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || report.Objects != 0 {
			t.Errorf("report = %+v, want the scanned head held", report)
		}
		assertPresent(t, ds, head)
	})

	t.Run("without a pointer an expired leftover is settled", func(t *testing.T) {
		mem := NewMemory()
		ds := newHiveRetention(t, mem)
		live := writeRun(t, ds, "a", "x")
		gone := writeRun(t, ds, "b", "x")
		legacyize(t, mem, live, gone)
		// An interrupted expiry of the newest snapshot left a partition copy.
		putTombstone(t, mem, gone.ID, newTombstone(gone.ID, gone.Manifest))
		var leftover string
		for _, k := range storeKeys(t, mem) {
			if !strings.Contains(k, string(gone.ID)) || !strings.HasSuffix(k, manifestFile) {
				continue
			}
			if strings.Contains(k, "partitions/") {
				leftover = k
				continue
			}
			if err := mem.Delete(t.Context(), k); err != nil {
				t.Fatal(err)
			}
		}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(report.Settled, []DatasetSnapshotID{gone.ID}) || exists(t, mem, leftover) {
			t.Errorf("report = %+v, leftover present %v; want the expired leftover settled", report, exists(t, mem, leftover))
		}
	})

	t.Run("without a pointer an expiring head the listing half shows is held", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newHiveRetention(t, lister)
		older := writeRun(t, ds, "a", "x")
		head := writeRun(t, ds, "b", "x")
		legacyize(t, mem, older, head)
		putTombstone(t, mem, head.ID, newTombstone(head.ID, head.Manifest)) // misplaced
		// The listing shows a partition copy but omits the canonical one.
		for _, k := range storeKeys(t, mem) {
			if strings.Contains(k, string(head.ID)) && strings.HasSuffix(k, manifestFile) && !strings.Contains(k, "partitions/") {
				lister.hidden = append(lister.hidden, k)
			}
		}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || report.Objects != 0 {
			t.Errorf("report = %+v, want the head held", report)
		}
		assertPresent(t, ds, head)
	})

	t.Run("a pointer missing beside new-form IDs stops the run", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		if err := mem.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}

		if _, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace}); !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("Sweep = %v, want ErrHeadUncommitted", err)
		}
	})

	t.Run("a held tombstone keeps its data", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		writeRecord(t, ds, 1)
		// A misplaced tombstone names the pointer's snapshot, whose manifest is
		// gone: the guard holds it back, and its data must stay too.
		held := idAt(time.Now().Add(-48 * time.Hour))
		data := "datasets/events/snapshots/" + string(held) + "/data/x"
		putObject(t, store, data)
		putTombstone(t, store, held, foreignTombstone(held, nil))
		setPointer(t, store, string(held))

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || report.Objects != 0 || !exists(t, store, data) {
			t.Errorf("report = %+v, want the held snapshot and its data untouched", report)
		}
	})
}

// -----------------------------------------------------------------------------
// Rejected tombstones
// -----------------------------------------------------------------------------

func TestSweep_RejectedTombstone(t *testing.T) {
	t.Run("invalid ones settle nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)
		third := writeRecord(t, ds, 3)
		writeRecord(t, ds, 4) // head
		prefix := "datasets/events/expired/"
		invalid := []string{
			prefix + string(first.ID),  // records another snapshot
			prefix + string(second.ID), // zero-length
			prefix + string(third.ID),  // no inventory
		}
		putTombstone(t, store, first.ID, newTombstone("123", first.Manifest))
		if err := store.Put(t.Context(), invalid[1], strings.NewReader("")); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(t.Context(), invalid[2], strings.NewReader(`{"snapshot_id":"`+string(third.ID)+`"}`)); err != nil {
			t.Fatal(err)
		}
		// Not snapshot-ID names, so not tombstones: neither reported nor acted on.
		victim := "datasets/events/snapshots/nested/self/manifest.json"
		putObject(t, store, victim)
		outOfScope := map[string]string{
			prefix + "nested/self": `{"snapshot_id":"nested/self"}`,
			prefix + "x.lock":      "",
			prefix + "00123":       `{"snapshot_id":"00123"}`, // generateID never pads
			prefix + "+123":        `{"snapshot_id":"+123"}`,
		}
		for p, body := range outOfScope {
			if err := store.Put(t.Context(), p, strings.NewReader(body)); err != nil {
				t.Fatal(err)
			}
		}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(invalid)
		if !slices.Equal(report.Unparseable, invalid) || len(report.Settled) != 0 {
			t.Errorf("report = %+v, want invalid tombstones %v and nothing settled", report, invalid)
		}
		if !exists(t, store, victim) {
			t.Error("a non-tombstone under expired/ steered a delete")
		}
		assertPresent(t, ds, first)
		assertPresent(t, ds, second)
		assertPresent(t, ds, third)
	})

	t.Run("same files with another commit instant settles nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		live := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		earlier := *live.Manifest
		earlier.CreatedAt = earlier.CreatedAt.Add(-time.Hour)
		putTombstone(t, store, live.ID, newTombstone(live.ID, &earlier))

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || len(report.Unparseable) != 1 {
			t.Errorf("report = %+v, want nothing settled and the tombstone reported", report)
		}
		assertPresent(t, ds, live)
	})

	t.Run("another inventory settles nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		live := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		// A tombstone left by an earlier snapshot with the live one's ID.
		putTombstone(t, store, live.ID, foreignTombstone(live.ID, []FileRef{{Path: "datasets/events/snapshots/" + string(live.ID) + "/data/old", SizeBytes: 1}}))

		for _, dryRun := range []bool{true, false} {
			report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace, DryRun: dryRun})
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"datasets/events/expired/" + string(live.ID)}
			if len(report.Settled) != 0 || report.Objects != 0 || !slices.Equal(report.Unparseable, want) {
				t.Errorf("Sweep(dry=%v) = %+v, want nothing settled and the tombstone reported", dryRun, report)
			}
		}
		assertPresent(t, ds, live)
	})

	t.Run("one a partition copy contradicts settles nothing", func(t *testing.T) {
		store := NewMemory()
		ds := newHiveRetention(t, store)
		run := writeRun(t, ds, "a", "x", "y")
		writeRun(t, ds, "b", "x")
		// An interrupted expiry left the canonical copy; a partition copy left by
		// an earlier snapshot with the same ID lists other objects.
		putTombstone(t, store, run.ID, newTombstone(run.ID, run.Manifest))
		stray := "datasets/events/partitions/run_id=a/event_type=y/segments/" + string(run.ID) + "/manifest.json"
		other := *run.Manifest
		other.Files = []FileRef{{Path: "datasets/events/partitions/run_id=a/event_type=y/segments/" + string(run.ID) + "/data/old", SizeBytes: 1}}
		rewriteManifest(t, store, stray, &other)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || report.Objects != 0 || !exists(t, store, stray) {
			t.Errorf("report = %+v, want nothing settled and every copy kept", report)
		}
		assertPresent(t, ds, run)
	})

	t.Run("its data is held past grace", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		writeRecord(t, ds, 1)
		// No manifest is left and the data is past grace, so it would age out
		// as an orphan; the invalid tombstone holds it instead.
		old := idAt(time.Now().Add(-48 * time.Hour))
		data := "datasets/events/snapshots/" + string(old) + "/data/x"
		putObject(t, store, data)
		tomb := "datasets/events/expired/" + string(old)
		if err := store.Put(t.Context(), tomb, strings.NewReader(`{"snapshot_id":"`+string(old)+`"}`)); err != nil {
			t.Fatal(err)
		}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if report.Objects != 0 || !exists(t, store, data) || !slices.Equal(report.Unparseable, []string{tomb}) {
			t.Errorf("report = %+v, want the tombstone reported and its data kept", report)
		}
	})

	t.Run("one contradicting the head is reported", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		head := writeRecord(t, ds, 1)
		tomb := "datasets/events/expired/" + string(head.ID)
		putTombstone(t, store, head.ID, foreignTombstone(head.ID, []FileRef{{Path: "datasets/events/snapshots/" + string(head.ID) + "/data/old", SizeBytes: 1}}))

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || !slices.Equal(report.Unparseable, []string{tomb}) {
			t.Errorf("report = %+v, want the head kept and its contradicted tombstone reported", report)
		}
		assertPresent(t, ds, head)
	})
}
