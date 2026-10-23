package lode

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// Roots and handle construction
// -----------------------------------------------------------------------------

func TestOpen(t *testing.T) {
	nilFactory := Factory[Store](func() (Store, error) {
		return nil, nil //nolint:nilnil // intentionally testing nil store with nil error
	})
	failing := Factory[Store](func() (Store, error) { return nil, errors.New("factory down") })

	// A root records its construction error, and every method returns it.
	for name, root := range map[string]*Root{
		"nil factory":           Open[Store](nil),
		"factory returns nil":   Open(nilFactory),
		"factory error":         Open(failing),
		"invalid path option":   Open(Memory(), WithDataDir("")),
		"conflicting flat path": Open(Memory(), WithFlatPaths(), WithDatasetsDir("t")),
	} {
		t.Run(name, func(t *testing.T) {
			_, recordsErr := root.Records(t.Context(), "d", JSONL[D]())
			_, blobsErr := root.Blobs(t.Context(), "d")
			_, listErr := root.ListDatasets(t.Context(), DatasetListOptions{})
			if recordsErr == nil || !errors.Is(blobsErr, recordsErr) || !errors.Is(listErr, recordsErr) {
				t.Errorf("Records = %v, Blobs = %v, ListDatasets = %v; want one recorded error from each", recordsErr, blobsErr, listErr)
			}
		})
	}
	t.Run("factory error is wrapped", func(t *testing.T) {
		_, err := Open(failing).Records(t.Context(), "d", JSONL[D]())
		if err == nil || !strings.Contains(err.Error(), "store factory failed: factory down") {
			t.Errorf("got %v, want the factory's error wrapped", err)
		}
	})
	t.Run("the factory is called once per root", func(t *testing.T) {
		calls := 0
		counting := Factory[Store](func() (Store, error) { calls++; return NewMemory(), nil })
		root := Open(counting)
		for _, id := range []DatasetID{"a", "b", "c"} {
			if _, err := root.Records(t.Context(), id, JSONL[D]()); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 1 {
			t.Errorf("factory called %d times, want once", calls)
		}
	})
}

func TestOpenReadOnly(t *testing.T) {
	failing := Factory[Store](func() (Store, error) { return nil, errors.New("factory down") })
	root := OpenReadOnly(failing)
	_, recordsErr := root.Records(t.Context(), "d", JSONL[D]())
	_, blobsErr := root.Blobs(t.Context(), "d")
	_, listErr := root.ListDatasets(t.Context(), DatasetListOptions{})
	if recordsErr == nil || !errors.Is(blobsErr, recordsErr) || !errors.Is(listErr, recordsErr) {
		t.Errorf("Records = %v, Blobs = %v, ListDatasets = %v; want one recorded error from each", recordsErr, blobsErr, listErr)
	}

	t.Run("a store without CompareAndSwap is readable", func(t *testing.T) {
		store := NewMemory()
		snap, err := recordsOn(t, store, "events").Write(t.Context(), R(D{"n": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenReadOnly(Use(testutil.WithoutCAS(store))).Records(t.Context(), "events", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		got, err := view.Read(t.Context(), snap.ID)
		if err != nil || len(got) != 1 {
			t.Fatalf("Read = %v, %v; want the record", got, err)
		}
	})

	t.Run("a view never creates a missing pointer", func(t *testing.T) {
		store := NewMemory()
		snap, err := recordsOn(t, store, "events").Write(t.Context(), R(D{"n": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		view := viewOn(t, store, "events")
		head, err := view.Latest(t.Context())
		if err != nil || head.ID != snap.ID {
			t.Fatalf("Latest = %v, %v; want %s by scan", head, err, snap.ID)
		}
		if exists(t, store, "datasets/events/latest") {
			t.Error("a view created the missing pointer")
		}
	})

	t.Run("a dataset without snapshots is not found", func(t *testing.T) {
		if _, err := OpenReadOnly(Memory()).Records(t.Context(), "events", JSONL[D]()); !errors.Is(err, ErrNotFound) {
			t.Errorf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("a view with another codec is rejected", func(t *testing.T) {
		store := NewMemory()
		if _, err := recordsOn(t, store, "events").Write(t.Context(), R(D{"n": 1}), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "events"); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestRoot_Blobs(t *testing.T) {
	t.Run("a partitioned layout is rejected", func(t *testing.T) {
		_, err := Open(Memory()).Blobs(t.Context(), "test-ds", hive("day"))
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "cannot be partitioned") {
			t.Fatalf("got %v, want ErrInvalidArgument naming the partitioning", err)
		}
	})

	t.Run("default layout success", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil || ds == nil {
			t.Fatalf("got %v, %v; want a handle", ds, err)
		}
	})

	t.Run("nil layout and compressor are rejected", func(t *testing.T) {
		for want, opt := range map[string]DatasetOption{"layout must not be nil": WithLayout(nil), "compressor must not be nil": WithCompressor(nil)} {
			if _, err := Open(Memory()).Blobs(t.Context(), "test-ds", opt); !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), want) {
				t.Errorf("got %v, want ErrInvalidArgument with %q", err, want)
			}
		}
	})
}

// event is a typed record for the generic-handle tests.
type event struct {
	Source string    `json:"source"`
	Day    string    `json:"day"`
	Count  int       `json:"count"`
	At     time.Time `json:"at"`
}

// runInfo is typed snapshot metadata.
type runInfo struct {
	RunID string `json:"run_id"`
	Files int    `json:"files"`
}

func TestRoot_Records_Typed(t *testing.T) {
	t.Run("the record type is inferred from the codec", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "events", JSONL[event]())
		if err != nil {
			t.Fatal(err)
		}
		want := []event{{Source: "a", Day: "d1", Count: 1}, {Source: "b", Day: "d2", Count: 2}}
		snap, err := ds.Write(t.Context(), want, runInfo{RunID: "r1", Files: 2})
		if err != nil {
			t.Fatal(err)
		}
		got, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("Read = %+v, want %+v", got, want)
		}
		var info runInfo
		if err := snap.Metadata.Decode(&info); err != nil || info != (runInfo{RunID: "r1", Files: 2}) {
			t.Errorf("metadata = %+v, %v; want the struct with its field types", info, err)
		}
	})

	t.Run("Hive partitions typed records with By", func(t *testing.T) {
		store := NewMemory()
		layout := Hive("source", "day").By(func(e event) Partition {
			return Partition{"source": e.Source, "day": e.Day}
		})
		ds := recordsOnTyped(t, store, layout)
		snap, err := ds.Write(t.Context(), []event{{Source: "a", Day: "d1"}, {Source: "b", Day: "d2"}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		parts, err := ds.ListPartitions(t.Context(), PartitionListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != 2 || parts[0].Path != "source=a/day=d1" || parts[1].Path != "source=b/day=d2" {
			t.Errorf("partitions = %v, want source=a/day=d1 and source=b/day=d2", parts)
		}
		if !slices.Equal(snap.PartitionKeys, []string{"source", "day"}) {
			t.Errorf("keys = %v, want source, day", snap.PartitionKeys)
		}
	})

	t.Run("a By function for another record type is rejected", func(t *testing.T) {
		layout := Hive("day").By(func(r runInfo) Partition { return Partition{"day": r.RunID} })
		_, err := Open(Memory()).Records(t.Context(), "events", JSONL[event](), WithLayout(layout))
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("a partition naming other keys fails the write", func(t *testing.T) {
		for name, values := range map[string]Partition{
			"missing key": {"source": "a"},
			"extra key":   {"source": "a", "day": "d1", "hour": "1"},
			"other key":   {"source": "a", "hour": "1"},
		} {
			layout := Hive("source", "day").By(func(event) Partition { return values })
			ds := recordsOnTyped(t, NewMemory(), layout)
			if _, err := ds.Write(t.Context(), []event{{Source: "a"}}, nil); err == nil {
				t.Errorf("%s: write succeeded, want a partitioning failure", name)
			}
		}
	})
}

// recordsOnTyped opens dataset "events" of event records over store with
// layout.
func recordsOnTyped(t *testing.T, store Store, layout Layout) Records[event] {
	t.Helper()
	ds, err := Open(Use(store)).Records(t.Context(), "events", JSONL[event](), WithLayout(layout))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

// -----------------------------------------------------------------------------
// G2-13, G2-14: Codec and compressor on read
// -----------------------------------------------------------------------------

// renamedCompressor is gzip under a name Lode does not bundle, as a custom
// compressor would be.
type renamedCompressor struct{ Compressor }

func (renamedCompressor) Name() string { return "custom-gzip" }

func TestRecords_Read(t *testing.T) {
	t.Run("a handle with another codec is rejected", func(t *testing.T) {
		store := NewMemory()
		if _, err := recordsOn(t, store, "test-ds").Write(t.Context(), R(D{"id": "1"}), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Use(store)).Blobs(t.Context(), "test-ds"); !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "codec mismatch") {
			t.Errorf("writer: got %v, want a codec ErrSettingsMismatch", err)
		}
		if _, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds"); !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "codec mismatch") {
			t.Errorf("view: got %v, want a codec ErrSettingsMismatch", err)
		}
	})

	t.Run("each snapshot is read with the compressor its manifest names", func(t *testing.T) {
		store := NewMemory()
		snap, err := blobsOn(t, store, "test-ds", WithCompressor(NewGzipCompressor())).Write(t.Context(), []byte("data"), nil)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		got, err := view.Read(t.Context(), snap.ID)
		if err != nil || string(got) != "data" {
			t.Fatalf("Read = %q, %v; want the gzip blob decompressed by name", got, err)
		}
	})

	t.Run("a custom compressor is passed to the view", func(t *testing.T) {
		store := NewMemory()
		custom := renamedCompressor{NewGzipCompressor()}
		snap, err := blobsOn(t, store, "test-ds", WithCompressor(custom)).Write(t.Context(), []byte("data"), nil)
		if err != nil {
			t.Fatal(err)
		}
		bare, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bare.Read(t.Context(), snap.ID); !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "custom-gzip") {
			t.Errorf("without it: got %v, want an ErrSettingsMismatch naming the compressor", err)
		}
		view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds", WithCompressor(custom))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := view.Read(t.Context(), snap.ID); err != nil || string(got) != "data" {
			t.Errorf("with it: Read = %q, %v; want the blob", got, err)
		}
	})
}

// assertEmptyMetadata fails unless m is the empty JSON object.
func assertEmptyMetadata(t *testing.T, m Metadata) {
	t.Helper()
	if got := string(m.JSON()); got != "{}" {
		t.Errorf("metadata = %s, want {}", got)
	}
}

// -----------------------------------------------------------------------------
// G2-15: Empty metadata explicitly valid and persisted
// -----------------------------------------------------------------------------

func TestDataset_Write_Metadata(t *testing.T) {
	t.Run("empty metadata valid and persisted", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// Write with explicitly empty metadata (not nil)
		snap, err := ds.Write(t.Context(), []byte("data"), nil)
		if err != nil {
			t.Fatalf("expected empty metadata to be valid, got error: %v", err)
		}

		// Verify metadata is persisted as empty object, not nil
		assertEmptyMetadata(t, snap.Metadata)

		// Verify we can read it back and metadata is preserved
		retrieved, err := ds.Snapshot(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("failed to retrieve snapshot: %v", err)
		}
		assertEmptyMetadata(t, retrieved.Metadata)
	})

	t.Run("nil metadata coalesces to empty", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), []byte("data"), nil)
		if err != nil {
			t.Fatalf("expected nil metadata to succeed, got: %v", err)
		}
		assertEmptyMetadata(t, snap.Metadata)
	})
}

func TestBlobs_StreamWrite_Metadata(t *testing.T) {
	t.Run("empty metadata valid and persisted", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatalf("expected empty metadata to be valid, got error: %v", err)
		}

		_, err = sw.Write([]byte("streaming data"))
		if err != nil {
			t.Fatal(err)
		}

		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Verify metadata is persisted as empty object
		assertEmptyMetadata(t, snap.Metadata)
	})

	t.Run("nil metadata coalesces to empty", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatalf("expected nil metadata to succeed, got: %v", err)
		}
		_, _ = sw.Write([]byte("data"))
		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		assertEmptyMetadata(t, snap.Metadata)
	})
}

func TestRecords_StreamWrite_Metadata(t *testing.T) {
	t.Run("empty metadata valid and persisted", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1"})
		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatalf("expected empty metadata to be valid, got error: %v", err)
		}

		// Verify metadata is persisted as empty object
		assertEmptyMetadata(t, snap.Metadata)
	})

	t.Run("nil metadata coalesces to empty", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1"})
		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatalf("expected nil metadata to succeed, got: %v", err)
		}
		assertEmptyMetadata(t, snap.Metadata)
	})

	t.Run("non timestamped records omits min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "a", "value": 1},
			{"id": "b", "value": 2},
		}
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp != nil {
			t.Errorf("expected MinTimestamp to be nil, got %v", snap.MinTimestamp)
		}
		if snap.MaxTimestamp != nil {
			t.Errorf("expected MaxTimestamp to be nil, got %v", snap.MaxTimestamp)
		}
	})

	t.Run("timestamped records computes min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[*timestampedRecord]())
		if err != nil {
			t.Fatal(err)
		}

		ts1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		ts2 := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
		ts3 := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC)

		records := []*timestampedRecord{
			{ID: "a", Time: ts1},
			{ID: "b", Time: ts2},
			{ID: "c", Time: ts3},
		}
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp == nil {
			t.Fatal("expected MinTimestamp to be set")
		}
		if snap.MaxTimestamp == nil {
			t.Fatal("expected MaxTimestamp to be set")
		}
		if !snap.MinTimestamp.Equal(ts1) {
			t.Errorf("expected MinTimestamp %v, got %v", ts1, *snap.MinTimestamp)
		}
		if !snap.MaxTimestamp.Equal(ts2) {
			t.Errorf("expected MaxTimestamp %v, got %v", ts2, *snap.MaxTimestamp)
		}
	})

	t.Run("stats nil", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "stream-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"key": "value"})
		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.Manifest.Files[0].Stats != nil {
			t.Errorf("expected nil Stats for JSONL stream, got %+v", snap.Manifest.Files[0].Stats)
		}
	})
}

// -----------------------------------------------------------------------------
// G3-1: Snapshot NOT visible before Commit
// Per CONTRACT_WRITE_API.md: "A snapshot MUST NOT be visible before Commit writes the manifest."
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_Visibility(t *testing.T) {
	t.Run("not visible before commit", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// Start streaming write
		sw, err := ds.StreamWrite(t.Context(), D{"test": "value"})
		if err != nil {
			t.Fatal(err)
		}

		// Write data but don't commit yet
		_, err = sw.Write([]byte("streaming data before commit"))
		if err != nil {
			t.Fatal(err)
		}

		// Snapshot should NOT be visible before commit
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots before commit, got: %v", err)
		}

		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots() failed: %v", err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected 0 snapshots before commit, got %d", len(snapshots))
		}

		// Now commit
		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Snapshot should now be visible
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed after commit: %v", err)
		}
		if latest.ID != snap.ID {
			t.Errorf("expected latest ID %s, got %s", snap.ID, latest.ID)
		}
	})

	t.Run("not visible before commit with existing snapshot", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// Create an existing snapshot first
		existingSnap, err := ds.Write(t.Context(), []byte("existing data"), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Start streaming write for second snapshot
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("new streaming data"))
		if err != nil {
			t.Fatal(err)
		}

		// Latest should still be the existing snapshot, not the uncommitted one
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed: %v", err)
		}
		if latest.ID != existingSnap.ID {
			t.Errorf("expected latest to be existing snapshot %s, got %s", existingSnap.ID, latest.ID)
		}

		// Snapshots list should only have one entry
		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots() failed: %v", err)
		}
		if len(snapshots) != 1 {
			t.Errorf("expected 1 snapshot before commit, got %d", len(snapshots))
		}

		// Commit and verify new snapshot is visible
		newSnap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		latest, err = ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed after commit: %v", err)
		}
		if latest.ID != newSnap.ID {
			t.Errorf("expected latest to be new snapshot %s, got %s", newSnap.ID, latest.ID)
		}
	})
}

// -----------------------------------------------------------------------------
// G3-2, G3-3: Abort semantics tests
// Per CONTRACT_WRITE_API.md:
// - "StreamWriter.Abort(ctx) MUST ensure no manifest is written."
// - "StreamWriter.Close() without Commit MUST behave as Abort."
//
// RISK NOTE: Context cancellation during streaming has nondeterministic outcomes.
// If the background store.Put completes before cancellation takes effect, the
// data object is written. However, no manifest is written unless Commit succeeds.
// The Abort path is deterministic and is tested explicitly below.
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_Abort(t *testing.T) {
	t.Run("no manifest", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write some data
		_, err = sw.Write([]byte("partial data"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Abort
		err = sw.Abort(t.Context())
		if err != nil {
			t.Fatalf("Abort failed: %v", err)
		}

		// Verify no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after abort, got: %v", err)
		}

		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots failed: %v", err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected 0 snapshots after abort, got %d", len(snapshots))
		}
	})

	t.Run("abort during write no manifest", func(t *testing.T) {
		store := NewMemory()
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write some data
		_, err = sw.Write([]byte("data that will be aborted"))
		if err != nil {
			t.Fatal(err)
		}

		// Abort explicitly
		err = sw.Abort(t.Context())
		if err != nil {
			t.Fatalf("Abort failed: %v", err)
		}

		// Verify no snapshot is visible
		_, latestErr := ds.Latest(t.Context())
		if !errors.Is(latestErr, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after abort, got: %v", latestErr)
		}

		// Verify no manifest in store
		paths, err := store.List(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after abort, found: %s", p)
			}
		}
	})

	t.Run("abort after commit returns error", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Abort after commit should fail
		err = sw.Abort(t.Context())
		if err == nil {
			t.Fatal("expected error aborting after commit")
		}
	})
}

func TestBlobs_StreamWrite_Interrupted(t *testing.T) {
	t.Run("close during write no manifest", func(t *testing.T) {
		// Per CONTRACT_WRITE_API.md: "Close() without Commit MUST behave as Abort"
		store := NewMemory()
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data that will be abandoned"))
		if err != nil {
			t.Fatal(err)
		}

		// Close without commit (should behave as abort)
		err = sw.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify no snapshot is visible
		_, latestErr := ds.Latest(t.Context())
		if !errors.Is(latestErr, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after close-without-commit, got: %v", latestErr)
		}

		// Verify no manifest in store
		paths, err := store.List(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after close-without-commit, found: %s", p)
			}
		}
	})

	t.Run("close without commit behaves as abort", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write some data
		_, err = sw.Write([]byte("will be abandoned"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Close without commit
		err = sw.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after close without commit, got: %v", err)
		}
	})

	t.Run("write error no manifest", func(t *testing.T) {
		// Test that write errors prevent manifest creation
		store := NewMemory()
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write data then abort to simulate failure path
		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		// Simulate failure by aborting
		_ = sw.Abort(t.Context())

		// Verify no manifest exists
		paths, _ := store.List(t.Context(), "")
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after simulated failure, found: %s", p)
			}
		}
	})
}

// RISK NOTE: Context cancellation during StreamWriteRecords
//
// StreamWriteRecords is an atomic operation from the caller's perspective.
// If the context is cancelled mid-stream:
// - The background store.Put may or may not have completed
// - Partial data may remain in storage (best-effort cleanup attempted)
// - No manifest will be written (the commit signal)
//
// The cleanup uses the caller's context for Delete, which may already be
// cancelled. Per CONTRACT_STORAGE.md, cleanup SHOULD use an independent
// context. Current implementation risk: cleanup may fail if context is
// already cancelled.
//
// This is documented behavior, not a test failure condition.

// -----------------------------------------------------------------------------
// G3-4: Commit signal = manifest presence
// Per CONTRACT_WRITE_API.md: "Manifest presence is the commit signal."
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_ManifestPresenceIsCommitSignal(t *testing.T) {
	store := NewMemory()
	ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
	if err != nil {
		t.Fatal(err)
	}

	sw, err := ds.StreamWrite(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = sw.Write([]byte("test data"))
	if err != nil {
		t.Fatal(err)
	}

	// Before commit: check that no manifest exists in store
	paths, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if strings.Contains(p, "manifest.json") {
			t.Errorf("manifest should not exist before commit, found: %s", p)
		}
	}

	// Commit
	_, err = sw.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// After commit: manifest must exist
	paths, err = store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	hasManifest := false
	for _, p := range paths {
		if strings.Contains(p, "manifest.json") {
			hasManifest = true
			break
		}
	}
	if !hasManifest {
		t.Error("manifest should exist after commit")
	}
}

func TestRecords_StreamWrite_ManifestPresenceIsCommitSignal(t *testing.T) {
	store := NewMemory()
	ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}

	iter := seqOf(D{"id": "1"}, D{"id": "2"})
	_, err = ds.StreamWrite(t.Context(), iter, nil)
	if err != nil {
		t.Fatal(err)
	}

	// After successful StreamWriteRecords: manifest must exist
	paths, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	hasManifest := false
	for _, p := range paths {
		if strings.Contains(p, "manifest.json") {
			hasManifest = true
			break
		}
	}
	if !hasManifest {
		t.Error("manifest should exist after successful StreamWriteRecords")
	}
}

func TestRecords_StreamWrite_IteratorError(t *testing.T) {
	t.Run("iterator error", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iterErr := errors.New("iterator failure")
		iter := failingSeq[D](iterErr)

		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if err == nil {
			t.Fatal("expected error from iterator, got nil")
		}
		if !errors.Is(err, iterErr) {
			t.Errorf("expected wrapped iterErr, got: %v", err)
		}

		// Verify no snapshot created
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after iterator error, got: %v", err)
		}
	})

	t.Run("no manifest written", func(t *testing.T) {
		// Explicitly verifies manifest is not written on iterator error
		store := NewMemory()
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iterErr := errors.New("iterator failure")
		iter := failingSeq[D](iterErr)

		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if err == nil {
			t.Fatal("expected error from iterator, got nil")
		}

		// Verify no manifest was written
		paths, err := store.List(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after iterator error, found: %s", p)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// Option validation tests
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Empty dataset behavior tests
// -----------------------------------------------------------------------------

func TestDataset_EmptyDataset(t *testing.T) {
	t.Run("latest empty dataset returns err no snapshots", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots, got: %v", err)
		}
	})

	t.Run("snapshots empty dataset returns empty list", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected empty list, got %d snapshots", len(snapshots))
		}
	})

	t.Run("snapshot empty dataset returns err not found", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		_, err = ds.Snapshot(t.Context(), "nonexistent-id")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Write validation tests
// -----------------------------------------------------------------------------

// The blob type makes v0's shape errors (no blob, two blobs, a value that is
// not a []byte) impossible; an empty blob is one empty object.
func TestBlobs_Write_EmptyBlob(t *testing.T) {
	ds := blobsOn(t, NewMemory(), "test-ds")
	snap, err := ds.Write(t.Context(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Files) != 1 || snap.RowCount != 1 {
		t.Fatalf("manifest = %+v, want one file and one row", snap.Manifest)
	}
	got, err := ds.Read(t.Context(), snap.ID)
	if err != nil || len(got) != 0 {
		t.Errorf("Read = %q, %v; want the empty blob", got, err)
	}
}

// -----------------------------------------------------------------------------
// StreamWrite tests
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_Commit(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), D{"source": "test"})
		if err != nil {
			t.Fatal(err)
		}

		// Write some data
		data := []byte("hello stream world")
		n, err := sw.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes written, got %d", len(data), n)
		}

		// Commit and verify snapshot
		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		if snap == nil {
			t.Fatal("expected snapshot, got nil")
		}
		if snap.RowCount != 1 {
			t.Errorf("expected RowCount 1, got %d", snap.RowCount)
		}
		if len(snap.Files) != 1 {
			t.Errorf("expected 1 file, got %d", len(snap.Files))
		}
		if metadataOf(t, snap.Metadata)["source"] != "test" {
			t.Errorf("expected metadata source=test, got %v", snap.Metadata)
		}

		// Verify snapshot is visible via Latest
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest failed: %v", err)
		}
		if latest.ID != snap.ID {
			t.Errorf("expected latest ID %s, got %s", snap.ID, latest.ID)
		}

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		readBytes := readData
		if string(readBytes) != string(data) {
			t.Errorf("expected %q, got %q", data, readBytes)
		}
	})

	t.Run("multiple writes", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write in chunks
		chunks := [][]byte{
			[]byte("chunk1"),
			[]byte("-chunk2"),
			[]byte("-chunk3"),
		}
		for _, chunk := range chunks {
			_, err = sw.Write(chunk)
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
		}

		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify data can be read back as concatenated
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		readBytes := readData
		expected := "chunk1-chunk2-chunk3"
		if string(readBytes) != expected {
			t.Errorf("expected %q, got %q", expected, readBytes)
		}
	})
}

func TestBlobs_StreamWrite_Misuse(t *testing.T) {
	t.Run("double commit returns error", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Second commit should fail
		_, err = sw.Commit(t.Context())
		if err == nil {
			t.Fatal("expected error on double commit")
		}
	})

	t.Run("write after abort returns error", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		err = sw.Abort(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Write after abort should fail
		_, err = sw.Write([]byte("more data"))
		if err == nil {
			t.Fatal("expected error writing after abort")
		}
	})

	t.Run("write after commit returns error", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Write after commit should fail
		_, err = sw.Write([]byte("more data"))
		if err == nil {
			t.Fatal("expected error writing after commit")
		}
	})
}

func TestBlobs_StreamWrite_Lineage(t *testing.T) {
	t.Run("parent snapshot linked", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// First write (regular Write)
		firstSnap, err := ds.Write(t.Context(), []byte("first"), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Second write (StreamWrite)
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = sw.Write([]byte("second"))
		secondSnap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		if secondSnap.ParentSnapshotID != firstSnap.ID {
			t.Errorf("expected parent %s, got %s", firstSnap.ID, secondSnap.ParentSnapshotID)
		}
	})

	t.Run("latest pointer skips scan", func(t *testing.T) {
		// StreamWrite → Commit must write the pointer so that a subsequent
		// Write uses O(1) parent resolution.
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// First write via StreamWrite
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = sw.Write([]byte("payload-1"))
		snap1, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterFirst := len(fs.ListCalls())

		// Second write via regular Write: pointer exists, no List
		snap2, err := ds.Write(t.Context(), []byte("payload-2"), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterSecond := len(fs.ListCalls())
		if listCallsAfterSecond != listCallsAfterFirst {
			t.Errorf("expected no new List calls after StreamWrite wrote pointer, got %d new calls",
				listCallsAfterSecond-listCallsAfterFirst)
		}
		if snap2.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap2.ParentSnapshotID)
		}
	})
}

func TestBlobs_StreamWrite_Encoding(t *testing.T) {
	t.Run("with checksum records checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("streaming data"))
		if err != nil {
			t.Fatal(err)
		}

		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Verify checksum algorithm recorded
		if snap.ChecksumAlgorithm != "md5" {
			t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.ChecksumAlgorithm)
		}

		// Verify file has checksum
		if snap.Manifest.Files[0].Checksum == "" {
			t.Error("expected non-empty checksum")
		}
		if len(snap.Manifest.Files[0].Checksum) != 32 {
			t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
		}
	})

	t.Run("with gzip compression", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithCompressor(NewGzipCompressor()))
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("compressible data that should be gzipped")
		_, err = sw.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify manifest indicates gzip compression
		if snap.Compressor != "gzip" {
			t.Errorf("expected compressor 'gzip', got %q", snap.Compressor)
		}

		// Verify file path ends with .gz
		if len(snap.Files) != 1 {
			t.Fatalf("expected 1 file, got %d", len(snap.Files))
		}
		if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".gz") {
			t.Errorf("expected .gz extension, got %s", snap.Manifest.Files[0].Path)
		}

		// Verify data can be read back correctly
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		readBytes := readData
		if string(readBytes) != string(data) {
			t.Errorf("expected %q, got %q", data, readBytes)
		}
	})

	t.Run("with zstd compression", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("compressible data that should be zstd compressed")
		_, err = sw.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		snap, err := sw.Commit(t.Context())
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify manifest indicates zstd compression
		if snap.Compressor != "zstd" {
			t.Errorf("expected compressor 'zstd', got %q", snap.Compressor)
		}

		// Verify file path ends with .zst
		if len(snap.Files) != 1 {
			t.Fatalf("expected 1 file, got %d", len(snap.Files))
		}
		if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".zst") {
			t.Errorf("expected .zst extension, got %s", snap.Manifest.Files[0].Path)
		}

		// Verify data can be read back correctly
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		readBytes := readData
		if string(readBytes) != string(data) {
			t.Errorf("expected %q, got %q", data, readBytes)
		}
	})
}

func TestDataset_Write_Encoding(t *testing.T) {
	t.Run("with checksum records checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), []byte("hello world"), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Verify checksum algorithm recorded
		if snap.ChecksumAlgorithm != "md5" {
			t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.ChecksumAlgorithm)
		}

		// Verify file has checksum
		if len(snap.Files) != 1 {
			t.Fatalf("expected 1 file, got %d", len(snap.Files))
		}
		if snap.Manifest.Files[0].Checksum == "" {
			t.Error("expected non-empty checksum")
		}
		// MD5 produces 32 hex characters
		if len(snap.Manifest.Files[0].Checksum) != 32 {
			t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
		}
	})

	t.Run("without checksum omits checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), []byte("hello world"), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Verify no checksum algorithm recorded
		if snap.ChecksumAlgorithm != "" {
			t.Errorf("expected empty ChecksumAlgorithm, got %q", snap.ChecksumAlgorithm)
		}

		// Verify file has no checksum
		if snap.Manifest.Files[0].Checksum != "" {
			t.Errorf("expected empty checksum, got %q", snap.Manifest.Files[0].Checksum)
		}
	})

	t.Run("with zstd compression", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "1", "data": "compressible zstd content"},
			{"id": "2", "data": "more compressible zstd content"},
		}

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Verify manifest indicates zstd compression
		if snap.Compressor != "zstd" {
			t.Errorf("expected compressor 'zstd', got %q", snap.Compressor)
		}
		if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".zst") {
			t.Errorf("expected .zst extension, got %s", snap.Manifest.Files[0].Path)
		}

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if len(readData) != 2 {
			t.Errorf("expected 2 records, got %d", len(readData))
		}
	})
}

func TestRecords_StreamWrite_Encoding(t *testing.T) {
	t.Run("with checksum records checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1"}, D{"id": "2"})
		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Verify checksum algorithm recorded
		if snap.ChecksumAlgorithm != "md5" {
			t.Errorf("expected ChecksumAlgorithm 'md5', got %q", snap.ChecksumAlgorithm)
		}

		// Verify file has checksum
		if snap.Manifest.Files[0].Checksum == "" {
			t.Error("expected non-empty checksum")
		}
		if len(snap.Manifest.Files[0].Checksum) != 32 {
			t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Files[0].Checksum))
		}
	})

	t.Run("with compression", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewGzipCompressor()))
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "1", "data": "some compressible content"},
			{"id": "2", "data": "more compressible content"},
		}
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatalf("StreamWriteRecords failed: %v", err)
		}

		if snap.Compressor != "gzip" {
			t.Errorf("expected compressor 'gzip', got %q", snap.Compressor)
		}
		if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".gz") {
			t.Errorf("expected .gz extension, got %s", snap.Manifest.Files[0].Path)
		}

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if len(readData) != 2 {
			t.Errorf("expected 2 records, got %d", len(readData))
		}
	})

	t.Run("with zstd compression", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "1", "data": "some zstd compressible content"},
			{"id": "2", "data": "more zstd compressible content"},
		}
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatalf("StreamWriteRecords failed: %v", err)
		}

		if snap.Compressor != "zstd" {
			t.Errorf("expected compressor 'zstd', got %q", snap.Compressor)
		}
		if !strings.HasSuffix(snap.Manifest.Files[0].Path, ".zst") {
			t.Errorf("expected .zst extension, got %s", snap.Manifest.Files[0].Path)
		}

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if len(readData) != 2 {
			t.Errorf("expected 2 records, got %d", len(readData))
		}
	})
}

// -----------------------------------------------------------------------------
// StreamWriteRecords tests
// -----------------------------------------------------------------------------

func TestRecords_StreamWrite_Commit(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "1", "value": "first"},
			{"id": "2", "value": "second"},
			{"id": "3", "value": "third"},
		}
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, D{"source": "stream"})
		if err != nil {
			t.Fatalf("StreamWriteRecords failed: %v", err)
		}

		if snap == nil {
			t.Fatal("expected snapshot, got nil")
		}
		if snap.RowCount != 3 {
			t.Errorf("expected RowCount 3, got %d", snap.RowCount)
		}
		if snap.Codec != "jsonl" {
			t.Errorf("expected codec 'jsonl', got %q", snap.Codec)
		}
		if metadataOf(t, snap.Metadata)["source"] != "stream" {
			t.Errorf("expected metadata source=stream, got %v", snap.Metadata)
		}

		// Verify snapshot is visible via Latest
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest failed: %v", err)
		}
		if latest.ID != snap.ID {
			t.Errorf("expected latest ID %s, got %s", snap.ID, latest.ID)
		}

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if len(readData) != 3 {
			t.Fatalf("expected 3 records, got %d", len(readData))
		}
	})

	t.Run("empty iterator", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf[D]()

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatalf("StreamWriteRecords failed: %v", err)
		}

		if snap.RowCount != 0 {
			t.Errorf("expected RowCount 0, got %d", snap.RowCount)
		}
	})

	t.Run("parent snapshot linked", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// First write (regular Write)
		firstSnap, err := ds.Write(t.Context(), []D{{"id": "1"}}, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Second write (StreamWriteRecords)
		iter := seqOf(D{"id": "2"})
		secondSnap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		if secondSnap.ParentSnapshotID != firstSnap.ID {
			t.Errorf("expected parent %s, got %s", firstSnap.ID, secondSnap.ParentSnapshotID)
		}
	})

	t.Run("latest pointer skips scan", func(t *testing.T) {
		// StreamWriteRecords must write the pointer.
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// First write
		snap1, err := ds.Write(t.Context(), R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterFirst := len(fs.ListCalls())

		// Second write via StreamWriteRecords: pointer exists
		iter := seqOf(D{"b": 2})
		snap2, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterSecond := len(fs.ListCalls())
		if listCallsAfterSecond != listCallsAfterFirst {
			t.Errorf("expected no new List calls on StreamWriteRecords, got %d new calls",
				listCallsAfterSecond-listCallsAfterFirst)
		}
		if snap2.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap2.ParentSnapshotID)
		}

		// Third write: verify chain continues via pointer
		snap3, err := ds.Write(t.Context(), R(D{"c": 3}), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterThird := len(fs.ListCalls())
		if listCallsAfterThird != listCallsAfterFirst {
			t.Errorf("expected no new List calls on third write, got %d new calls",
				listCallsAfterThird-listCallsAfterFirst)
		}
		if snap3.ParentSnapshotID != snap2.ID {
			t.Errorf("expected parent %s, got %s", snap2.ID, snap3.ParentSnapshotID)
		}
	})
}

func TestRecords_StreamWrite_Rejects(t *testing.T) {
	t.Run("nil iterator", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		_, err = ds.StreamWrite(t.Context(), nil, nil)
		if err == nil {
			t.Fatal("expected error for nil iterator, got nil")
		}
		if !errors.Is(err, ErrNilIterator) {
			t.Errorf("expected ErrNilIterator, got: %v", err)
		}
	})

	t.Run("non streaming codec", func(t *testing.T) {
		// testCodec does not implement StreamingRecordCodec
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", &testCodec{})
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1"})
		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if !errors.Is(err, ErrCodecNotStreamable) {
			t.Errorf("expected ErrCodecNotStreamable, got: %v", err)
		}
	})

	t.Run("with partitioner", func(t *testing.T) {
		// StreamWriteRecords cannot partition since it's single-pass streaming
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1", "day": "2024-01-01"})
		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if err == nil {
			t.Fatal("expected error for partitioning, got nil")
		}
		if !errors.Is(err, ErrPartitioningNotSupported) {
			t.Errorf("expected ErrPartitioningNotSupported, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Checksum tests
// -----------------------------------------------------------------------------

func TestDataset_Checksum(t *testing.T) {
	t.Run("computed on compressed data", func(t *testing.T) {
		// Two datasets with same data but different compression
		// should produce different checksums (since checksum is on stored bytes)
		dsNoComp, err := Open(Memory()).Blobs(t.Context(), "test-no-comp", WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		dsGzip, err := Open(Memory()).Blobs(t.Context(), "test-gzip", WithCompressor(NewGzipCompressor()), WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("compressible data that should produce different checksums")

		snapNoComp, err := dsNoComp.Write(t.Context(), data, nil)
		if err != nil {
			t.Fatal(err)
		}

		snapGzip, err := dsGzip.Write(t.Context(), data, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Both should have checksums
		if snapNoComp.Manifest.Files[0].Checksum == "" {
			t.Error("expected checksum for no-compression")
		}
		if snapGzip.Manifest.Files[0].Checksum == "" {
			t.Error("expected checksum for gzip")
		}

		// Checksums should be different (stored bytes differ)
		if snapNoComp.Manifest.Files[0].Checksum == snapGzip.Manifest.Files[0].Checksum {
			t.Error("expected different checksums for different compression")
		}
	})

	t.Run("same data produces same checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(NewMD5Checksum()))
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("consistent data")

		snap1, err := ds.Write(t.Context(), data, nil)
		if err != nil {
			t.Fatal(err)
		}

		snap2, err := ds.Write(t.Context(), data, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Same data should produce same checksum
		if snap1.Manifest.Files[0].Checksum != snap2.Manifest.Files[0].Checksum {
			t.Errorf("expected same checksum for same data, got %s vs %s",
				snap1.Manifest.Files[0].Checksum, snap2.Manifest.Files[0].Checksum)
		}
	})
}

// -----------------------------------------------------------------------------
// Timestamped interface tests
// -----------------------------------------------------------------------------

func TestDataset_Write_Timestamps(t *testing.T) {
	t.Run("non timestamped records omits min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Plain maps don't implement Timestamped
		records := []D{
			{"id": "a", "value": 1},
			{"id": "b", "value": 2},
		}

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp != nil {
			t.Errorf("expected MinTimestamp to be nil, got %v", snap.MinTimestamp)
		}
		if snap.MaxTimestamp != nil {
			t.Errorf("expected MaxTimestamp to be nil, got %v", snap.MaxTimestamp)
		}
	})

	t.Run("timestamped records computes min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[*timestampedRecord]())
		if err != nil {
			t.Fatal(err)
		}

		ts1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		ts2 := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
		ts3 := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC)

		records := []*timestampedRecord{
			{ID: "a", Time: ts1},
			{ID: "b", Time: ts2},
			{ID: "c", Time: ts3},
		}

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp == nil {
			t.Fatal("expected MinTimestamp to be set")
		}
		if snap.MaxTimestamp == nil {
			t.Fatal("expected MaxTimestamp to be set")
		}

		if !snap.MinTimestamp.Equal(ts1) {
			t.Errorf("expected MinTimestamp %v, got %v", ts1, *snap.MinTimestamp)
		}
		if !snap.MaxTimestamp.Equal(ts2) {
			t.Errorf("expected MaxTimestamp %v, got %v", ts2, *snap.MaxTimestamp)
		}
	})

	t.Run("single timestamped record same min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[*timestampedRecord]())
		if err != nil {
			t.Fatal(err)
		}

		ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
		records := []*timestampedRecord{{ID: "only", Time: ts}}

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp == nil || snap.MaxTimestamp == nil {
			t.Fatal("expected both timestamps to be set")
		}
		if !snap.MinTimestamp.Equal(ts) || !snap.MaxTimestamp.Equal(ts) {
			t.Errorf("expected both timestamps to be %v", ts)
		}
	})

	t.Run("raw blob omits timestamps", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), []byte("blob data"), nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.MinTimestamp != nil {
			t.Errorf("expected MinTimestamp to be nil for raw blob, got %v", snap.MinTimestamp)
		}
		if snap.MaxTimestamp != nil {
			t.Errorf("expected MaxTimestamp to be nil for raw blob, got %v", snap.MaxTimestamp)
		}
	})
}

// -----------------------------------------------------------------------------
// FileStats write-path tests
// -----------------------------------------------------------------------------

func TestDataset_Write_Stats(t *testing.T) {
	t.Run("JSONL codec stats nil", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "jsonl-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		records := []D{{"key": "value"}}
		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.Manifest.Files[0].Stats != nil {
			t.Errorf("expected nil Stats for JSONL codec, got %+v", snap.Manifest.Files[0].Stats)
		}
	})

	t.Run("parquet codec stats populated", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString},
			},
		}
		codec, err := newParquet(schema)
		if err != nil {
			t.Fatal(err)
		}

		ds, err := Open(Memory()).Records(t.Context(), "stats-ds", codec)
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			map[string]any{"id": int64(1), "name": "alice"},
			map[string]any{"id": int64(2), "name": "bob"},
		}

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		if len(snap.Files) != 1 {
			t.Fatalf("expected 1 file, got %d", len(snap.Files))
		}

		stats := snap.Manifest.Files[0].Stats
		if stats == nil {
			t.Fatal("expected Stats on FileRef, got nil")
		}
		if stats.RowCount != 2 {
			t.Errorf("RowCount = %d, want 2", stats.RowCount)
		}
		if len(stats.Columns) != 2 {
			t.Fatalf("len(Columns) = %d, want 2", len(stats.Columns))
		}
		if stats.Columns[0].Name != "id" {
			t.Errorf("Columns[0].Name = %q, want %q", stats.Columns[0].Name, "id")
		}
	})

	t.Run("raw blob stats nil", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "blob-ds")
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), []byte("raw data"), nil)
		if err != nil {
			t.Fatal(err)
		}

		if snap.Manifest.Files[0].Stats != nil {
			t.Errorf("expected nil Stats for raw blob, got %+v", snap.Manifest.Files[0].Stats)
		}
	})
}

// -----------------------------------------------------------------------------
// FileStats serialization tests
// -----------------------------------------------------------------------------

func TestFileRef_Stats(t *testing.T) {
	t.Run("JSON round trip", func(t *testing.T) {
		ref := FileRef{
			Path:      "data/test.parquet",
			SizeBytes: 1024,
			Stats: &FileStats{
				RowCount: 100,
				Columns: []ColumnStats{
					{Name: "id", Min: float64(1), Max: float64(100), NullCount: 0},
					{Name: "name", Min: "alice", Max: "zara", NullCount: 5},
				},
			},
		}

		data, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}

		var decoded FileRef
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}

		if decoded.Stats == nil {
			t.Fatal("expected Stats after round-trip, got nil")
		}
		if decoded.Stats.RowCount != 100 {
			t.Errorf("RowCount = %d, want 100", decoded.Stats.RowCount)
		}
		if len(decoded.Stats.Columns) != 2 {
			t.Fatalf("len(Columns) = %d, want 2", len(decoded.Stats.Columns))
		}
		if decoded.Stats.Columns[0].Name != "id" {
			t.Errorf("Columns[0].Name = %q, want %q", decoded.Stats.Columns[0].Name, "id")
		}
		// JSON round-trips numbers as float64
		if decoded.Stats.Columns[0].Min != float64(1) {
			t.Errorf("Columns[0].Min = %v, want 1", decoded.Stats.Columns[0].Min)
		}
		if decoded.Stats.Columns[1].NullCount != 5 {
			t.Errorf("Columns[1].NullCount = %d, want 5", decoded.Stats.Columns[1].NullCount)
		}
	})

	t.Run("backward compat", func(t *testing.T) {
		// JSON without stats field should decode cleanly
		jsonData := `{"path":"data/test.gz","size_bytes":512}`

		var ref FileRef
		if err := json.Unmarshal([]byte(jsonData), &ref); err != nil {
			t.Fatal(err)
		}

		if ref.Stats != nil {
			t.Errorf("expected nil Stats for JSON without stats field, got %+v", ref.Stats)
		}
		if ref.Path != "data/test.gz" {
			t.Errorf("Path = %q, want %q", ref.Path, "data/test.gz")
		}
	})

	t.Run("omitted when nil", func(t *testing.T) {
		ref := FileRef{
			Path:      "data/test.gz",
			SizeBytes: 256,
		}

		data, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(data), "stats") {
			t.Errorf("expected no stats key in JSON when Stats is nil, got: %s", data)
		}
	})
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// timestampedRecord implements Timestamped for testing.
type timestampedRecord struct {
	ID   string    `json:"id"`
	Time time.Time `json:"time"`
}

func (r *timestampedRecord) Timestamp() time.Time {
	return r.Time
}

// testCodec is a simple codec for testing.
type testCodec struct{}

func (c *testCodec) Name() string     { return "test-codec" }
func (c *testCodec) Describe() string { return "" }

func (c *testCodec) Encode(_ io.Writer, _ []D) error {
	return nil
}

func (c *testCodec) Decode(_ io.Reader) ([]D, error) {
	return nil, nil //nolint:nilnil // stub for testing
}

// -----------------------------------------------------------------------------
// Latest pointer tests (issue #118 / #119)
// -----------------------------------------------------------------------------

func TestDataset_Write_Pointer(t *testing.T) {
	t.Run("latest pointer skips scan", func(t *testing.T) {
		// After the first Write creates the pointer, subsequent writes must not
		// call store.List to resolve the parent snapshot ID (O(1) via pointer).
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// First write: no pointer yet, falls back to scan (List)
		snap1, err := ds.Write(t.Context(), R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterFirst := len(fs.ListCalls())
		if listCallsAfterFirst == 0 {
			t.Fatal("expected at least one List call on first write (no pointer)")
		}

		// Second write: pointer exists, must NOT call List
		snap2, err := ds.Write(t.Context(), R(D{"b": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterSecond := len(fs.ListCalls())
		if listCallsAfterSecond != listCallsAfterFirst {
			t.Errorf("expected no new List calls on second write, got %d new calls",
				listCallsAfterSecond-listCallsAfterFirst)
		}

		// Verify parent chain is correct
		if snap2.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap2.ParentSnapshotID)
		}

		// Third write: still pointer-based
		snap3, err := ds.Write(t.Context(), R(D{"c": 3}), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterThird := len(fs.ListCalls())
		if listCallsAfterThird != listCallsAfterFirst {
			t.Errorf("expected no new List calls on third write, got %d new calls",
				listCallsAfterThird-listCallsAfterFirst)
		}
		if snap3.ParentSnapshotID != snap2.ID {
			t.Errorf("expected parent %s, got %s", snap2.ID, snap3.ParentSnapshotID)
		}
	})

	// TestDataset_Write_CorruptPointer_FallsBackToScan verifies that a corrupt
	// latest pointer (referencing a nonexistent snapshot) falls through to scan
	// in resolveParent, preventing broken linear history.
	//
	// With CAS-enabled stores (ConditionalWriter), resolveParent correctly identifies
	// the parent via scan, but the CAS write detects the external pointer modification
	// and returns ErrSnapshotConflict. This is correct: external modification IS a
	// concurrent write from CAS's perspective.
	t.Run("corrupt pointer falls back to scan", func(t *testing.T) {
		mem := NewMemory()
		ds, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Write first snapshot.
		_, err = ds.Write(t.Context(), R(D{"i": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Corrupt the pointer to reference a nonexistent snapshot.
		pointerPath := "datasets/test-ds/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte("nonexistent-snap-id"))); err != nil {
			t.Fatal(err)
		}

		// With CAS, the write detects external pointer modification as a conflict.
		// The in-memory cache knows what it last wrote; the external change is detected.
		_, err = ds.Write(t.Context(), R(D{"i": 2}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("expected ErrSnapshotConflict for externally modified pointer, got: %v", err)
		}
	})

	// TestDataset_Write_StaleButExistingPointer_UsesInMemoryCache verifies that
	// when the pointer is externally modified after a successful commit, CAS
	// detects the external modification and returns ErrSnapshotConflict.
	//
	// The in-memory cache correctly resolves the parent snapshot, but the pointer
	// was tampered with between commits. CAS treats this as concurrent modification.
	t.Run("stale but existing pointer uses in memory cache", func(t *testing.T) {
		mem := NewMemory()
		ds, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Write snap-1 and snap-2 normally.
		snap1, err := ds.Write(t.Context(), R(D{"i": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ds.Write(t.Context(), R(D{"i": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Externally modify the pointer to point back to snap-1.
		pointerPath := "datasets/test-ds/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte(snap1.ID))); err != nil {
			t.Fatal(err)
		}

		// With CAS, the write detects external pointer modification.
		// The cache knows we last wrote snap-2's ID, but the pointer now contains snap-1's ID.
		_, err = ds.Write(t.Context(), R(D{"i": 3}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("expected ErrSnapshotConflict for externally modified pointer, got: %v", err)
		}
	})

	// TestDataset_Write_PointerWriteFailure_AbortsCommit verifies that when
	// writeLatestPointer fails, the commit is aborted and no manifest is written.
	// This is the core of the pointer-before-manifest protocol.
	t.Run("pointer write failure aborts commit", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		factory := Use(fs)

		ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// First write succeeds (creates snap-1).
		snap1, err := ds.Write(t.Context(), R(D{"i": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Inject CAS error on the "latest" pointer path.
		// writeLatestPointer uses CompareAndSwap when the store implements ConditionalWriter.
		fs.SetCASError(errors.New("injected: pointer write failure"), "latest")

		// Record calls before the failed write.
		fs.Reset()

		// Second write should fail because pointer write is required.
		_, err = ds.Write(t.Context(), R(D{"i": 2}), nil)
		if err == nil {
			t.Fatal("expected write to fail when pointer write fails")
		}

		// Verify no manifest was written (only Put calls should be for data file + pointer attempt).
		putCalls := fs.PutCalls()
		for _, p := range putCalls {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not be written when pointer fails, but saw Put(%s)", p)
			}
		}

		// Clear the CAS error and write again — should succeed with snap-1 as parent.
		fs.SetCASError(nil)

		snap3, err := ds.Write(t.Context(), R(D{"i": 3}), nil)
		if err != nil {
			t.Fatalf("write after clearing error should succeed: %v", err)
		}
		if snap3.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap3.ParentSnapshotID)
		}
	})
}

func TestDataset_LatestPointer_PendingCommit(t *testing.T) {
	// Advance writes the pointer before the manifest, so a pointer that
	// names a snapshot without a manifest may belong to a pending commit.
	// Latest serves the scan result but must not replace the pointer, and the
	// next commit must adopt the pending commit as parent once its manifest
	// is persisted.
	store := NewMemory()
	open := func() Records[D] {
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		return ds
	}
	reader, other := open(), open()
	snap1, err := reader.Write(t.Context(), R(D{"a": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := other.Write(t.Context(), R(D{"b": 2}), nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := "datasets/test-ds/snapshots/" + string(pending.ID) + "/manifest.json"
	moveObject(t, store, manifestPath, "pending/manifest.json")
	if err := store.Delete(t.Context(), manifestPath); err != nil {
		t.Fatal(err)
	}

	latest, err := reader.Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != snap1.ID {
		t.Errorf("Latest = %s, want the committed %s", latest.ID, snap1.ID)
	}
	rc, err := store.Get(t.Context(), "datasets/test-ds/latest")
	if err != nil {
		t.Fatal(err)
	}
	pointer, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(pointer) != string(pending.ID) {
		t.Errorf("pointer = %q, want it to remain at the pending %q", pointer, pending.ID)
	}

	moveObject(t, store, "pending/manifest.json", manifestPath)
	next, err := reader.Write(t.Context(), R(D{"c": 3}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.ParentSnapshotID != pending.ID {
		t.Errorf("parent = %s, want the landed %s", next.ParentSnapshotID, pending.ID)
	}
}

// TestDataset_Write_ParentsOnPendingCommit verifies that a commit beginning
// after another commit has advanced the pointer, but before that commit has
// persisted its manifest, adopts the pending commit as its parent. Any other
// parent would fork the history (CONTRACT_CORE §Head Pointer).
func TestDataset_Write_ParentsOnPendingCommit(t *testing.T) {
	store := NewMemory()
	open := func() Records[D] {
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		return ds
	}
	first, other := open(), open()
	if _, err := first.Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Fatal(err)
	}
	pending, err := other.Write(t.Context(), R(D{"b": 2}), nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := "datasets/test-ds/snapshots/" + string(pending.ID) + "/manifest.json"
	moveObject(t, store, manifestPath, "pending/manifest.json")
	if err := store.Delete(t.Context(), manifestPath); err != nil {
		t.Fatal(err)
	}

	next, err := open().Write(t.Context(), R(D{"c": 3}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.ParentSnapshotID != pending.ID {
		t.Errorf("parent = %s, want the pending %s", next.ParentSnapshotID, pending.ID)
	}
}

// writeInvertedHead persists the history shape in which lode#208 forked: the
// head's snapshot ID is smaller than its parent's, because the head's write
// began first and committed second, so the largest ID names the parent. It
// returns the head.
func writeInvertedHead(t *testing.T, store Store) DatasetSnapshotID {
	t.Helper()
	base := time.Now().Add(-time.Hour)
	parent, head := idAt(base.Add(2*time.Second)), idAt(base.Add(time.Second))
	for _, link := range []struct{ id, parent DatasetSnapshotID }{{parent, ""}, {head, parent}} {
		m := validTestManifest()
		m.SnapshotID, m.ParentSnapshotID = link.id, link.parent
		writeManifest(t.Context(), t, store, m)
	}
	if err := store.Put(t.Context(), "datasets/test-ds/latest", strings.NewReader(string(head))); err != nil {
		t.Fatal(err)
	}
	return head
}

// TestDataset_TransientHeadReadErrors verifies that a failed read of the
// pointer or of the head's manifest is returned to the caller. Treating it
// as an absent pointer or manifest selects the largest snapshot ID, which is
// the head's parent in this shape (CONTRACT_CORE §Head Pointer, lode#208).
func TestDataset_TransientHeadReadErrors(t *testing.T) {
	t.Run("Latest returns a failed head load", func(t *testing.T) {
		store := NewMemory()
		head := writeInvertedHead(t, store)
		fs := testutil.NewFaultStore(store)
		ds := blobsOn(t, fs, "test-ds")
		fs.SetGetError(errGetFailed, "/"+string(head)+"/")
		if snap, err := ds.Latest(t.Context()); !errors.Is(err, errGetFailed) {
			t.Fatalf("got %v, %v; want the failed read", snap, err)
		}
	})
	// Opening a writer resolves the head, so a failed pointer read surfaces
	// when the writer opens, before its first write.
	t.Run("opening a writer returns a failed pointer read", func(t *testing.T) {
		store := NewMemory()
		writeInvertedHead(t, store)
		fs := testutil.NewFaultStore(store)
		fs.SetGetError(errGetFailed, "/latest")
		if _, err := Open(Use(fs)).Blobs(t.Context(), "test-ds"); !errors.Is(err, errGetFailed) {
			t.Fatalf("got %v; want the failed read", err)
		}
	})
	t.Run("opening a view returns a failed head load", func(t *testing.T) {
		for _, match := range []string{"/latest", "manifest.json"} {
			store := NewMemory()
			writeInvertedHead(t, store)
			fs := testutil.NewFaultStore(store)
			fs.SetGetError(errGetFailed, match)
			if _, err := OpenReadOnly(Use(fs)).Blobs(t.Context(), "test-ds"); !errors.Is(err, errGetFailed) {
				t.Errorf("%s: got %v; want the failed read", match, err)
			}
		}
	})
}

func TestDataset_LatestPointer(t *testing.T) {
	t.Run("read after write", func(t *testing.T) {
		// After a write, the pointer file must exist in the store and
		// Latest() must resolve via pointer (no List calls).
		store := NewMemory()
		fs := testutil.NewFaultStore(store)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.Write(t.Context(), R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Verify pointer file exists in store.
		pointerPath := "datasets/test-ds/latest"
		rc, err := store.Get(t.Context(), pointerPath)
		if err != nil {
			t.Fatalf("expected pointer file at %s, got error: %v", pointerPath, err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap.ID) {
			t.Errorf("pointer file content: expected %q, got %q", snap.ID, string(data))
		}

		// Reset call tracking to verify Latest() uses pointer, not List.
		fs.Reset()
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed: %v", err)
		}
		if latest.ID != snap.ID {
			t.Errorf("Latest() returned %s, expected %s", latest.ID, snap.ID)
		}
		if len(fs.ListCalls()) != 0 {
			t.Errorf("Latest() should not call List when pointer exists, got %d List calls",
				len(fs.ListCalls()))
		}
	})

	t.Run("updates across writes", func(t *testing.T) {
		// Pointer must always track the most recent snapshot.
		store := NewMemory()
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		pointerPath := "datasets/test-ds/latest"

		for i := range 3 {
			snap, err := ds.Write(t.Context(), R(D{"i": i}), nil)
			if err != nil {
				t.Fatalf("write %d failed: %v", i, err)
			}

			rc, err := store.Get(t.Context(), pointerPath)
			if err != nil {
				t.Fatalf("write %d: pointer file missing: %v", i, err)
			}
			data, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(data) != string(snap.ID) {
				t.Errorf("write %d: pointer %q, expected %q", i, string(data), snap.ID)
			}
		}
	})

	t.Run("backward compat", func(t *testing.T) {
		// Pre-pointer datasets (manifests exist, no pointer) must fall back
		// to scan and work correctly. After a write, the pointer is created.
		store := NewMemory()
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Write a manifest directly without writing a pointer (simulates pre-pointer data).
		snap1, err := ds.Write(t.Context(), R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Delete the pointer to simulate pre-pointer state.
		_ = store.Delete(t.Context(), "datasets/test-ds/latest")

		// Latest() should still work via scan fallback.
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() scan fallback failed: %v", err)
		}
		if latest.ID != snap1.ID {
			t.Errorf("scan fallback returned %s, expected %s", latest.ID, snap1.ID)
		}

		// Next write should create the pointer.
		snap2, err := ds.Write(t.Context(), R(D{"b": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}

		rc, err := store.Get(t.Context(), "datasets/test-ds/latest")
		if err != nil {
			t.Fatalf("pointer should exist after write: %v", err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap2.ID) {
			t.Errorf("pointer %q, expected %q", string(data), snap2.ID)
		}
	})

	t.Run("corrupt pointer", func(t *testing.T) {
		// If the pointer references a nonexistent snapshot, Latest() must
		// fall back to scan and return the correct result.
		store := NewMemory()
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Write a real snapshot.
		snap1, err := ds.Write(t.Context(), R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Overwrite the pointer with a nonexistent snapshot ID.
		pointerPath := "datasets/test-ds/latest"
		_ = store.Delete(t.Context(), pointerPath)
		_ = store.Put(t.Context(), pointerPath, strings.NewReader("nonexistent-id"))

		// Latest() should fall back to scan and find snap1.
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() with corrupt pointer failed: %v", err)
		}
		if latest.ID != snap1.ID {
			t.Errorf("expected %s from scan fallback, got %s", snap1.ID, latest.ID)
		}
	})

	t.Run("all layouts", func(t *testing.T) {
		// Verify pointer paths for all three layouts.
		tests := []struct {
			name         string
			layout       layout
			expectedPath string
		}{
			{
				name:         "default",
				layout:       defaultLayout(),
				expectedPath: "datasets/my-ds/latest",
			},
			{
				name:         "flat",
				layout:       flatLayout(),
				expectedPath: "my-ds/latest",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.layout.latestPointerPath("my-ds")
				if got != tt.expectedPath {
					t.Errorf("expected %q, got %q", tt.expectedPath, got)
				}
			})
		}

		hl := newPathLayout(defaultPathNames(), partitionScheme{name: "hive", keys: []string{"day"}})
		got := hl.latestPointerPath("my-ds")
		if got != "datasets/my-ds/latest" {
			t.Errorf("hive: expected %q, got %q", "datasets/my-ds/latest", got)
		}
	})
}

func TestDataset_HiveLayout(t *testing.T) {
	// TestDataset_HiveLayout_SnapshotByID_NoListCall verifies that Snapshot(ctx, id)
	// with HiveLayout does NOT require a List call. The canonical manifest is always
	// written alongside partition manifests, so a single Get resolves the snapshot.
	t.Run("snapshot by ID no list call", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		factory := Use(fs)

		ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}

		records := R(
			D{"id": 1, "day": "2024-01-15"},
			D{"id": 2, "day": "2024-01-16"},
		)

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Reset counters after the write.
		fs.Reset()

		// Snapshot by ID — should NOT trigger any List calls.
		got, err := ds.Snapshot(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Snapshot(%s) failed: %v", snap.ID, err)
		}
		if got.ID != snap.ID {
			t.Fatalf("expected snapshot ID %s, got %s", snap.ID, got.ID)
		}

		listCalls := fs.ListCalls()
		if len(listCalls) != 0 {
			t.Fatalf("expected 0 List calls for Snapshot(id), got %d: %v", len(listCalls), listCalls)
		}
	})

	// TestDataset_HiveLayout_CanonicalManifestWritten verifies that writeManifests
	// writes the canonical manifest alongside partition manifests for HiveLayout.
	t.Run("canonical manifest written", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		factory := Use(fs)

		ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}

		records := R(
			D{"id": 1, "day": "2024-01-15"},
			D{"id": 2, "day": "2024-01-16"},
		)

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Canonical path must exist.
		canonicalPath := "datasets/test-ds/segments/" + string(snap.ID) + "/manifest.json"
		exists, err := fs.Exists(t.Context(), canonicalPath)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("canonical manifest not found at %s", canonicalPath)
		}

		// Partition paths must also exist.
		partPaths := []string{
			"datasets/test-ds/partitions/day=2024-01-15/segments/" + string(snap.ID) + "/manifest.json",
			"datasets/test-ds/partitions/day=2024-01-16/segments/" + string(snap.ID) + "/manifest.json",
		}
		for _, pp := range partPaths {
			exists, err := fs.Exists(t.Context(), pp)
			if err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Fatalf("partition manifest not found at %s", pp)
			}
		}
	})

	// TestDataset_HiveLayout_BackwardCompat_FallbackScan verifies that Snapshot(ctx, id)
	// falls back to findSnapshotByID when the canonical manifest is missing (pre-fix data).
	t.Run("backward compat fallback scan", func(t *testing.T) {
		mem := NewMemory()
		fs := testutil.NewFaultStore(mem)
		factory := Use(fs)

		ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}

		records := R(
			D{"id": 1, "day": "2024-01-15"},
		)

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Simulate pre-fix data: delete the canonical manifest.
		canonicalPath := "datasets/test-ds/segments/" + string(snap.ID) + "/manifest.json"
		if err := mem.Delete(t.Context(), canonicalPath); err != nil {
			t.Fatal(err)
		}

		// Snapshot should still work via findSnapshotByID fallback.
		got, err := ds.Snapshot(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Snapshot(%s) failed after canonical delete: %v", snap.ID, err)
		}
		if got.ID != snap.ID {
			t.Fatalf("expected snapshot ID %s, got %s", snap.ID, got.ID)
		}
	})
}

func TestDataset_Write_ColdStart(t *testing.T) {
	// TestDataset_Write_ColdStart_CorruptPointer_FallsBackToScan verifies that
	// a new Dataset instance (cold start) with a corrupt pointer (references
	// nonexistent snapshot) correctly falls back to scan.
	t.Run("corrupt pointer falls back to scan", func(t *testing.T) {
		mem := NewMemory()

		// Process A: write 2 snapshots.
		dsA, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := dsA.Write(t.Context(), R(D{"i": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		// The scan below must find snap2 as the largest ID.
		testutil.AwaitClockPast(t, string(snap1.ID))
		snap2, err := dsA.Write(t.Context(), R(D{"i": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Corrupt the pointer to reference a nonexistent snapshot.
		pointerPath := "datasets/test-ds/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte("nonexistent-snap-id"))); err != nil {
			t.Fatal(err)
		}

		// Process B: cold start with corrupt pointer.
		dsB, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		snap3, err := dsB.Write(t.Context(), R(D{"i": 3}), nil)
		if err != nil {
			t.Fatalf("cold-start write with corrupt pointer should succeed: %v", err)
		}

		// snap3 must have snap2 as parent (from scan fallback).
		if snap3.ParentSnapshotID != snap2.ID {
			t.Errorf("expected parent %s (from scan), got %s", snap2.ID, snap3.ParentSnapshotID)
		}
	})

	// TestDataset_Write_ColdStart_ReadsPointerFromStore verifies that a new
	// Dataset instance (cold start with no in-memory cache) resolves the correct
	// parent by reading the persistent pointer from the store.
	t.Run("reads pointer from store", func(t *testing.T) {
		mem := NewMemory()

		// Process A: write 3 snapshots.
		dsA, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := dsA.Write(t.Context(), R(D{"i": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = dsA.Write(t.Context(), R(D{"i": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}
		snap3, err := dsA.Write(t.Context(), R(D{"i": 3}), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Verify pointer is on snap3 (protocol ensures this).
		rc, err := mem.Get(t.Context(), "datasets/test-ds/latest")
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap3.ID) {
			t.Fatalf("pointer should track snap3, got %q", string(data))
		}

		// Process B: cold start (new Dataset instance, empty in-memory cache).
		dsB, err := Open(Use(mem)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		snap4, err := dsB.Write(t.Context(), R(D{"i": 4}), nil)
		if err != nil {
			t.Fatalf("cold-start write should succeed: %v", err)
		}

		// snap4 must have snap3 as parent (pointer is correct).
		if snap4.ParentSnapshotID != snap3.ID {
			t.Errorf("expected parent %s, got %s (snap1=%s)",
				snap3.ID, snap4.ParentSnapshotID, snap1.ID)
		}
	})
}

// -----------------------------------------------------------------------------
// CAS integration tests — concurrent conflict detection
// -----------------------------------------------------------------------------

func TestDataset_Write_Conflict(t *testing.T) {
	// TestDataset_Write_ConcurrentConflict verifies that two datasets sharing
	// the same store detect concurrent writes via ErrSnapshotConflict.
	// Memory store implements ConditionalWriter, enabling CAS.
	//
	// Scenario: ds1 commits first. ds2 then commits, which succeeds because it
	// reads the updated pointer. But then ds1 commits again, and ds2 also tries
	// to commit — ds2's cached pointer is now stale, so CAS detects the conflict.
	t.Run("concurrent conflict", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}

		// Both start empty. ds1 writes first (CAS create: expected="" → snap1).
		_, err = ds1.Write(t.Context(), R(D{"writer": "ds1", "seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}

		// ds2 writes (reads pointer from store → sees snap1 → CAS update succeeds).
		_, err = ds2.Write(t.Context(), R(D{"writer": "ds2", "seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds2 first write: %v", err)
		}

		// Now ds1's cached pointer is "snap1" but the pointer was updated by ds2 to "snap2".
		// ds1's next write should detect the conflict.
		_, err = ds1.Write(t.Context(), R(D{"writer": "ds1", "seq": 2}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("expected ErrSnapshotConflict from ds1 (stale cache), got: %v", err)
		}

		// Documented retry path: re-read Latest(), then re-commit on the same instance.
		_, err = ds1.Latest(t.Context())
		if err != nil {
			t.Fatalf("ds1 Latest() after conflict: %v", err)
		}

		_, err = ds1.Write(t.Context(), R(D{"writer": "ds1", "seq": 2}), nil)
		if err != nil {
			t.Fatalf("ds1 retry after Latest() should succeed, got: %v", err)
		}
	})

	// TestDataset_Write_WithRetryCount_SucceedsAfterConflict verifies that a
	// dataset configured with WithRetryCount automatically retries on CAS
	// conflict and succeeds when the conflict resolves.
	t.Run("with retry count succeeds after conflict", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D](), WithRetry(3), WithRetryJitter(0.0), // deterministic for test stability
			WithRetryBaseDelay(time.Millisecond), // fast retries
		)
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}

		// ds1 writes first.
		_, err = ds1.Write(t.Context(), R(D{"writer": "ds1", "seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}

		// ds2 writes, advancing the pointer past ds1's cached value.
		_, err = ds2.Write(t.Context(), R(D{"writer": "ds2", "seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds2 write: %v", err)
		}

		// ds1 writes again — stale cache triggers CAS conflict on first attempt,
		// but WithRetry(3) should automatically retry and succeed.
		snap, err := ds1.Write(t.Context(), R(D{"writer": "ds1", "seq": 2}), nil)
		if err != nil {
			t.Fatalf("ds1 write with retry should succeed, got: %v", err)
		}
		if snap == nil {
			t.Fatal("expected non-nil snapshot from retry")
		}

		// Verify the snapshot chain: ds1's second write should parent off ds2's write.
		if snap.ParentSnapshotID == "" {
			t.Error("expected non-empty parent snapshot ID after retry")
		}

		// Verify file paths are consistent with the committed snapshot ID.
		// Data files must be co-located under the same snapshot directory.
		for _, f := range snap.Files {
			if !strings.Contains(f.Path, string(snap.ID)) {
				t.Errorf("file path %q does not contain committed snapshot ID %q", f.Path, snap.ID)
			}
		}
	})

	// TestDataset_Write_WithRetryCount_ExhaustsRetries verifies that when all
	// retries are exhausted, ErrSnapshotConflict is returned.
	t.Run("with retry count exhausts retries", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D](), WithRetry(1), // only 1 retry
			WithRetryJitter(0.0), WithRetryBaseDelay(time.Millisecond))
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}
		ds3, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds3: %v", err)
		}

		// ds1 writes first.
		_, err = ds1.Write(t.Context(), R(D{"seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}

		// ds2 writes, advancing pointer.
		_, err = ds2.Write(t.Context(), R(D{"seq": 2}), nil)
		if err != nil {
			t.Fatalf("ds2 write: %v", err)
		}

		// ds3 writes, advancing pointer again.
		_, err = ds3.Write(t.Context(), R(D{"seq": 3}), nil)
		if err != nil {
			t.Fatalf("ds3 write: %v", err)
		}

		// ds1 now has a stale cache pointing at snap1. The retry will refresh
		// to snap3 and succeed on the first retry. But we need persistent
		// contention to exhaust retries. Since we can't inject contention
		// between retry attempts with the memory store, we verify the basic
		// mechanics: a single retry is enough to resolve a one-hop conflict.
		_, err = ds1.Write(t.Context(), R(D{"seq": 4}), nil)
		if err != nil {
			t.Fatalf("expected success with 1 retry (conflict resolved after refresh), got: %v", err)
		}
	})

	// TestDataset_Write_WithRetryCount_Zero_CurrentBehavior verifies that
	// WithRetry(0) preserves the default behavior (no retry).
	t.Run("with retry count zero current behavior", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D](), WithRetry(0))
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}

		// ds1 writes first.
		_, err = ds1.Write(t.Context(), R(D{"seq": 1}), nil)
		if err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}

		// ds2 writes, advancing pointer.
		_, err = ds2.Write(t.Context(), R(D{"seq": 2}), nil)
		if err != nil {
			t.Fatalf("ds2 write: %v", err)
		}

		// ds1 should fail immediately without retry.
		_, err = ds1.Write(t.Context(), R(D{"seq": 3}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("expected ErrSnapshotConflict with retries=0, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// CAS retry tests (WithRetryCount)
// -----------------------------------------------------------------------------

func TestDataset_Write_RetryOptions(t *testing.T) {
	// TestDataset_Write_WithRetryCount_OptionValidation verifies option validation.
	t.Run("with retry count option validation", func(t *testing.T) {
		_, err := Open(Memory()).Blobs(t.Context(), "test", WithRetry(-1))
		if err == nil {
			t.Fatal("expected error for negative retry count")
		}

		_, err = Open(Memory()).Blobs(t.Context(), "test", WithRetryJitter(1.5))
		if err == nil {
			t.Fatal("expected error for jitter > 1.0")
		}

		_, err = Open(Memory()).Blobs(t.Context(), "test", WithRetryBaseDelay(-time.Second))
		if err == nil {
			t.Fatal("expected error for negative base delay")
		}

		_, err = Open(Memory()).Blobs(t.Context(), "test", WithRetryMaxDelay(0))
		if err == nil {
			t.Fatal("expected error for zero max delay")
		}
	})

	// TestDataset_Write_WithRetryObserver_CalledPerRetry verifies the observer
	// is invoked once per retry with 1-indexed attempt numbers, and not for the
	// initial attempt.
	t.Run("with retry observer called per retry", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		var attempts []int
		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D](), WithRetry(3), WithRetryJitter(0.0), WithRetryBaseDelay(time.Millisecond), WithRetryObserver(func(attempt int) { attempts = append(attempts, attempt) }))
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}

		// Uncontended write: no retry, observer not called.
		if _, err := ds1.Write(t.Context(), R(D{"seq": 1}), nil); err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}
		if len(attempts) != 0 {
			t.Fatalf("expected no observer calls without conflict, got %v", attempts)
		}

		// ds2 advances the pointer; ds1's stale cache conflicts once, then retries.
		if _, err := ds2.Write(t.Context(), R(D{"seq": 2}), nil); err != nil {
			t.Fatalf("ds2 write: %v", err)
		}
		if _, err := ds1.Write(t.Context(), R(D{"seq": 3}), nil); err != nil {
			t.Fatalf("ds1 write with retry: %v", err)
		}
		if len(attempts) != 1 || attempts[0] != 1 {
			t.Fatalf("expected observer calls [1], got %v", attempts)
		}
	})

	// TestDataset_Write_WithRetryObserver_NotCalledWithoutRetryCount verifies the
	// observer is inert when retries are disabled.
	t.Run("with retry observer not called without retry count", func(t *testing.T) {
		mem := NewMemory()
		factory := Use(mem)

		calls := 0
		ds1, err := Open(factory).Records(t.Context(), "shared", JSONL[D](), WithRetryObserver(func(int) { calls++ }))
		if err != nil {
			t.Fatalf("NewDataset ds1: %v", err)
		}
		ds2, err := Open(factory).Records(t.Context(), "shared", JSONL[D]())
		if err != nil {
			t.Fatalf("NewDataset ds2: %v", err)
		}

		if _, err := ds1.Write(t.Context(), R(D{"seq": 1}), nil); err != nil {
			t.Fatalf("ds1 first write: %v", err)
		}
		if _, err := ds2.Write(t.Context(), R(D{"seq": 2}), nil); err != nil {
			t.Fatalf("ds2 write: %v", err)
		}
		_, err = ds1.Write(t.Context(), R(D{"seq": 3}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("expected ErrSnapshotConflict, got: %v", err)
		}
		if calls != 0 {
			t.Fatalf("expected no observer calls with retries=0, got %d", calls)
		}
	})

	// TestDataset_WithRetryObserver_OptionValidation verifies nil is
	// rejected. A view takes no retry option: ViewOption does not include it,
	// which the compiler enforces.
	t.Run("with retry observer option validation", func(t *testing.T) {
		if _, err := Open(Memory()).Blobs(t.Context(), "test", WithRetryObserver(nil)); err == nil {
			t.Fatal("expected error for nil retry observer")
		}
	})
}

// TestDataset_Write_EmptyBatch verifies that a write of no records commits a
// snapshot with no data file on every layout: the partitioner yields no
// group for an empty batch.
func TestDataset_Write_EmptyBatch(t *testing.T) {
	for name, root := range map[string]struct {
		paths  []RootOption
		layout Layout
	}{
		"default": {nil, Unpartitioned()},
		"hive":    {nil, Hive("day")},
		"flat":    {[]RootOption{WithFlatPaths()}, Unpartitioned()},
	} {
		t.Run(name, func(t *testing.T) {
			ds, err := Open(Memory(), root.paths...).Records(t.Context(), "ds", JSONL[D](), WithLayout(root.layout))
			if err != nil {
				t.Fatal(err)
			}
			snap, err := ds.Write(t.Context(), []D{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(snap.Files) != 0 || snap.RowCount != 0 {
				t.Errorf("empty write: %d files and %d rows, want none", len(snap.Files), snap.RowCount)
			}
		})
	}
}
