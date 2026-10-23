package lode

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

const sweepGrace = 24 * time.Hour

// -----------------------------------------------------------------------------
// Requests
// -----------------------------------------------------------------------------

func TestSweep_Request(t *testing.T) {
	t.Run("non-positive grace is rejected", func(t *testing.T) {
		for _, grace := range []time.Duration{0, -time.Second} {
			fs := testutil.NewFaultStore(NewMemory())
			ds := newDefaultRetention(t, fs)
			fs.Reset() // opening the handle read the head
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
		setup := func(t *testing.T) (Store, Records[D]) {
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

	t.Run("a tombstone read failure still reports earlier invalid ones", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newDefaultRetention(t, lister)
		first := writeRecord(t, ds, 1)
		second := writeRecord(t, ds, 2)
		writeRecord(t, ds, 3)
		invalid := "datasets/events/expired/" + string(first.ID) // sorts first
		if err := mem.Put(t.Context(), invalid, strings.NewReader("{}")); err != nil {
			t.Fatal(err)
		}
		putTombstone(t, mem, second.ID, newTombstone(second.ID, second.Manifest))
		lister.failGet = "datasets/events/expired/" + string(second.ID)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if !errors.Is(err, errGetFailed) {
			t.Fatalf("Sweep = %v, want the read failure", err)
		}
		if !slices.Equal(report.Unparseable, []string{invalid}) {
			t.Errorf("Unparseable = %v, want the invalid tombstone examined first", report.Unparseable)
		}
	})

	t.Run("a delete failure reports completed work", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, hs)
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

	t.Run("an inventory object the listing omits is still removed", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newDefaultRetention(t, lister)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		putTombstone(t, mem, first.ID, newTombstone(first.ID, first.Manifest))
		data := first.Manifest.Files[0].Path
		lister.hidden = []string{data}

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if exists(t, mem, data) || report.Objects != 0 {
			t.Errorf("report = %+v, data present %v; want the unlisted object removed and not counted", report, exists(t, mem, data))
		}
	})

	t.Run("a snapshot the listing shows nothing of is left alone", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newDefaultRetention(t, lister)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		putTombstone(t, mem, first.ID, newTombstone(first.ID, first.Manifest)) // misplaced
		// A lagging listing omits every manifest and object of the snapshot.
		for _, k := range storeKeys(t, mem) {
			if strings.Contains(k, string(first.ID)) && !strings.Contains(k, "expired/") {
				lister.hidden = append(lister.hidden, k)
			}
		}
		lister.deleted = nil // the first commit removed its staged manifest

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || len(lister.deleted) != 0 {
			t.Errorf("report = %+v, deleted %v; want nothing touched", report, lister.deleted)
		}
		assertPresent(t, ds, first)
	})

	t.Run("an interrupted expiry", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newHiveRetention(t, hs)
		run := writeRun(t, ds, "a", "x", "y")
		writeRun(t, ds, "b", "x")
		stray := "datasets/events/partitions/run_id=a/event_type=x/segments/" + string(run.ID) + "/data/partial.bin"
		putObject(t, mem, stray)

		hs.onDelete = func(p string) error {
			if strings.Contains(p, "partitions/") && strings.HasSuffix(p, "manifest.json") {
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
		if report.Objects != 3 || report.Bytes != totalBytes(run.Files) || report.Unsized != 1 || report.Orphans != 0 {
			t.Errorf("report = %+v, want 3 objects, %d bytes, 1 unsized, 0 orphans", report, totalBytes(run.Files))
		}
		assertGone(t, mem, ds, run)
	})

	t.Run("an expiry with no manifest left", func(t *testing.T) {
		mem := NewMemory()
		hs := &hookStore{Store: mem}
		ds := newDefaultRetention(t, hs)
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
		if len(report.Settled) != 0 || report.Objects != len(first.Files) ||
			report.Bytes != totalBytes(first.Files) || report.Unsized != 0 {
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
		lister.deleted = nil // the first commit removed its staged manifest

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
		ds, err := Open(FS(dir)).Records(t.Context(), "events", JSONL[D]())
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

// TestSweep_HeadResolution covers how Sweep finds the head it holds: only
// the pointer names it.
func TestSweep_HeadResolution(t *testing.T) {
	t.Run("a pointer-less dataset is refused", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		older := writeRecord(t, ds, 1)
		head := writeRecord(t, ds, 2)
		legacyize(t, mem, older, head)
		putTombstone(t, mem, head.ID, newTombstone(head.ID, head.Manifest)) // misplaced

		if _, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace}); !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("Sweep = %v, want ErrHeadUncommitted", err)
		}
		assertPresent(t, ds, head)
	})

	t.Run("a pointer-less dataset of orphans only is swept", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		orphan := "datasets/events/snapshots/" + string(idAt(time.Now().Add(-48*time.Hour))) + "/data/x"
		putObject(t, mem, orphan)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil || report.Orphans != 1 || exists(t, mem, orphan) {
			t.Fatalf("Sweep = %+v, %v; want the orphan reclaimed", report, err)
		}
	})

	t.Run("the pointer's head keeps its objects when the listing omits its manifests", func(t *testing.T) {
		mem := NewMemory()
		lister := &listInjector{Store: mem}
		ds := newHiveRetention(t, mem)
		writeRun(t, ds, "a", "x")
		head := writeRun(t, ds, "b", "x")
		// A pre-canonical head: only its partition copy, which the listing
		// omits along with its data, and a stray object of its in another
		// partition.
		var copies []string
		for _, k := range storeKeys(t, mem) {
			if strings.Contains(k, string(head.ID)) && strings.HasSuffix(k, "manifest.json") {
				copies = append(copies, k)
			}
		}
		for _, f := range head.Files {
			lister.hidden = append(lister.hidden, f.Path) // the stray alone is listed
		}
		for _, k := range copies {
			if strings.Contains(k, "partitions/") {
				lister.hidden = append(lister.hidden, k)
				continue
			}
			if err := mem.Delete(t.Context(), k); err != nil {
				t.Fatal(err)
			}
		}
		stray := "datasets/events/partitions/run_id=z/event_type=x/segments/" + string(head.ID) + "/data/stray"
		putObject(t, mem, stray)
		// Sweep's own listing lags; the later lookup confirming the head
		// does not.
		lister.hideOnce = true
		ds = newHiveRetention(t, lister)

		if _, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Nanosecond}); err != nil {
			t.Fatal(err)
		}
		if !exists(t, mem, stray) {
			t.Error("an object of the pointer's head was reclaimed as an orphan")
		}
	})

	t.Run("a commit in flight settles nothing", func(t *testing.T) {
		mem := NewMemory()
		ds := newDefaultRetention(t, mem)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		putTombstone(t, mem, first.ID, newTombstone(first.ID, first.Manifest))
		setPointer(t, mem, string(idAt(time.Now()))) // its manifest is not written yet
		orphan := "datasets/events/snapshots/" + string(idAt(time.Now().Add(-48*time.Hour))) + "/data/x"
		putObject(t, mem, orphan)

		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: sweepGrace})
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Settled) != 0 || exists(t, mem, orphan) {
			t.Errorf("report = %+v, orphan present %v; want nothing settled and the orphan reclaimed", report, exists(t, mem, orphan))
		}
		assertPresent(t, ds, first)
	})

	t.Run("a pointer missing beside new-form IDs is refused", func(t *testing.T) {
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
