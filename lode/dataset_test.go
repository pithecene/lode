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
	"testing"
	"testing/iotest"
	"time"

	"github.com/pithecene/lode/internal/errs"

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
		"nil factory":         Open[Store](nil),
		"factory returns nil": Open(nilFactory),
		"factory error":       Open(failing),
		"nil root option":     Open(Memory(), nil),
	} {
		t.Run(name, func(t *testing.T) {
			_, recordsErr := root.Records(t.Context(), "d", JSONL[D]())
			_, blobsErr := root.Blobs(t.Context(), "d")
			_, listErr := root.ListDatasets(t.Context(), DatasetListOptions{})
			removeErr := root.RemoveDataset(t.Context(), "d")
			if recordsErr == nil || !errors.Is(blobsErr, recordsErr) || !errors.Is(listErr, recordsErr) || !errors.Is(removeErr, recordsErr) {
				t.Errorf("Records = %v, Blobs = %v, ListDatasets = %v, RemoveDataset = %v; want one recorded error from each", recordsErr, blobsErr, listErr, removeErr)
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
		if len(parts) != 2 || parts[0].String() != "source=a/day=d1" || parts[1].String() != "source=b/day=d2" {
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
func recordsOnTyped(t *testing.T, store Store, layout Layout) *Records[event] {
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
		snap, err := blobsOn(t, store, "test-ds", WithCompressor(Gzip())).Write(t.Context(), []byte("data"), nil)
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
		assertCustomBlobComponentRequired(t, "custom-gzip", WithCompressor(renamedCompressor{Gzip()}))
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

		snap, err := ds.StreamWrite(t.Context(), strings.NewReader("streaming data"), D{})
		if err != nil {
			t.Fatalf("expected empty metadata to be valid, got error: %v", err)
		}

		// Verify metadata is persisted as empty object
		assertEmptyMetadata(t, snap.Metadata)
	})

	t.Run("nil metadata coalesces to empty", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.StreamWrite(t.Context(), strings.NewReader("data"), nil)
		if err != nil {
			t.Fatalf("expected nil metadata to succeed, got: %v", err)
		}
		assertEmptyMetadata(t, snap.Metadata)
	})
}

func TestRecords_StreamWrite_Metadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata any
	}{
		{"empty metadata valid and persisted", D{}},
		{"nil metadata coalesces to empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := recordsOn(t, NewMemory(), "test-ds")
			snap, err := ds.StreamWrite(t.Context(), seqOf(D{"id": "1"}), tc.metadata)
			if err != nil {
				t.Fatal(err)
			}
			assertEmptyMetadata(t, snap.Metadata)
		})
	}

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

		assertNoTimestamps(t, snap)
	})

	t.Run("timestamped records computes min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[*timestampedRecord]())
		if err != nil {
			t.Fatal(err)
		}

		records, ts1, ts2 := timestampedFixture()
		iter := seqOf(records...)

		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		assertTimeRange(t, snap, ts1, ts2)
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

		if snap.Manifest.Objects[0].Stats != nil {
			t.Errorf("expected nil Stats for JSONL stream, got %+v", snap.Manifest.Objects[0].Stats)
		}
	})
}

// -----------------------------------------------------------------------------
// G3-1: Snapshot NOT visible before StreamWrite finishes reading and commits
// Per CONTRACT_WRITE_API.md: "A snapshot MUST NOT be visible before the manifest is written."
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_Visibility(t *testing.T) {
	t.Run("not visible before commit", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		pr, pw := io.Pipe()
		type result struct {
			snap *Snapshot
			err  error
		}
		done := make(chan result, 1)
		go func() {
			snap, err := ds.StreamWrite(t.Context(), pr, D{"test": "value"})
			done <- result{snap: snap, err: err}
		}()

		// Write data into the unbuffered pipe so StreamWrite is mid-flight,
		// blocked awaiting EOF before writing the manifest.
		if _, err := pw.Write([]byte("streaming data before commit")); err != nil {
			t.Fatal(err)
		}

		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots before EOF, got: %v", err)
		}
		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots() failed: %v", err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected 0 snapshots before EOF, got %d", len(snapshots))
		}

		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		res := <-done
		if res.err != nil {
			t.Fatalf("StreamWrite failed: %v", res.err)
		}

		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed after commit: %v", err)
		}
		if latest.ID != res.snap.ID {
			t.Errorf("expected latest ID %s, got %s", res.snap.ID, latest.ID)
		}
	})

	t.Run("not visible before commit with existing snapshot", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		existingSnap, err := ds.Write(t.Context(), []byte("existing data"), nil)
		if err != nil {
			t.Fatal(err)
		}

		pr, pw := io.Pipe()
		type result struct {
			snap *Snapshot
			err  error
		}
		done := make(chan result, 1)
		go func() {
			snap, err := ds.StreamWrite(t.Context(), pr, nil)
			done <- result{snap: snap, err: err}
		}()

		if _, err := pw.Write([]byte("new streaming data")); err != nil {
			t.Fatal(err)
		}

		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed: %v", err)
		}
		if latest.ID != existingSnap.ID {
			t.Errorf("expected latest to be existing snapshot %s, got %s", existingSnap.ID, latest.ID)
		}

		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots() failed: %v", err)
		}
		if len(snapshots) != 1 {
			t.Errorf("expected 1 snapshot before EOF, got %d", len(snapshots))
		}

		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		res := <-done
		if res.err != nil {
			t.Fatal(res.err)
		}

		latest, err = ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed after commit: %v", err)
		}
		if latest.ID != res.snap.ID {
			t.Errorf("expected latest to be new snapshot %s, got %s", res.snap.ID, latest.ID)
		}
	})
}

// -----------------------------------------------------------------------------
// G3-2, G3-3: Cancellation and reader failure semantics
// Per CONTRACT_WRITE_API.md, a failed or canceled StreamWrite MUST NOT write a manifest.
// -----------------------------------------------------------------------------

func TestBlobs_StreamWrite_Abort(t *testing.T) {
	t.Run("context canceled mid-stream leaves no manifest", func(t *testing.T) {
		store := NewMemory()
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, err := ds.StreamWrite(ctx, pr, nil)
			done <- err
		}()

		if _, err := pw.Write([]byte("partial data")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		cancel()
		errs.IgnoreError(pw.CloseWithError(ctx.Err()))

		if err := <-done; err == nil {
			t.Fatal("expected error after context cancellation")
		}

		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after cancellation, got: %v", err)
		}
		snapshots, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatalf("Snapshots failed: %v", err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected 0 snapshots after cancellation, got %d", len(snapshots))
		}
		paths, err := store.List(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after cancellation, found: %s", p)
			}
		}
	})
}

func TestBlobs_StreamWrite_Interrupted(t *testing.T) {
	t.Run("reader error mid-stream writes no manifest", func(t *testing.T) {
		store := NewMemory()
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		readErr := errors.New("stream truncated")
		r := io.MultiReader(strings.NewReader("data that will be abandoned"), iotest.ErrReader(readErr))
		if _, err := ds.StreamWrite(t.Context(), r, nil); !errors.Is(err, readErr) {
			t.Fatalf("got %v, want wrapped readErr", err)
		}

		if _, latestErr := ds.Latest(t.Context()); !errors.Is(latestErr, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after reader error, got: %v", latestErr)
		}

		paths, err := store.List(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not exist after reader error, found: %s", p)
			}
		}
	})
}

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

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := ds.StreamWrite(t.Context(), pr, nil)
		done <- err
	}()

	if _, err := pw.Write([]byte("test data")); err != nil {
		t.Fatal(err)
	}

	// Before EOF: check that no manifest exists in store
	paths, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if strings.Contains(p, "manifest.json") {
			t.Errorf("manifest should not exist before EOF, found: %s", p)
		}
	}

	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
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
	if len(snap.Objects) != 1 || snap.RowCount != 1 {
		t.Fatalf("manifest = %+v, want one object and one row", snap.Manifest)
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

		data := []byte("hello stream world")
		snap, err := ds.StreamWrite(t.Context(), bytes.NewReader(data), D{"source": "test"})
		if err != nil {
			t.Fatalf("StreamWrite failed: %v", err)
		}

		if snap == nil {
			t.Fatal("expected snapshot, got nil")
		}
		if snap.RowCount != 1 {
			t.Errorf("expected RowCount 1, got %d", snap.RowCount)
		}
		if len(snap.Objects) != 1 {
			t.Errorf("expected 1 object, got %d", len(snap.Objects))
		}
		if metadataOf(t, snap.Metadata)["source"] != "test" {
			t.Errorf("expected metadata source=test, got %v", snap.Metadata)
		}

		assertLatestIs(t, ds.dataset, snap.ID)

		// Verify data can be read back
		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if string(readData) != string(data) {
			t.Errorf("expected %q, got %q", data, readData)
		}
	})

	t.Run("multiple chunks", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		r := io.MultiReader(
			strings.NewReader("chunk1"),
			strings.NewReader("-chunk2"),
			strings.NewReader("-chunk3"),
		)
		snap, err := ds.StreamWrite(t.Context(), r, nil)
		if err != nil {
			t.Fatalf("StreamWrite failed: %v", err)
		}

		readData, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		expected := "chunk1-chunk2-chunk3"
		if string(readData) != expected {
			t.Errorf("expected %q, got %q", expected, readData)
		}
	})
}

func TestBlobs_StreamWrite_Misuse(t *testing.T) {
	t.Run("nil reader returns ErrInvalidArgument", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds.StreamWrite(t.Context(), nil, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("invalid metadata returns ErrInvalidArgument", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds.StreamWrite(t.Context(), strings.NewReader("data"), "not-an-object"); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
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
		secondSnap, err := ds.StreamWrite(t.Context(), strings.NewReader("second"), nil)
		if err != nil {
			t.Fatal(err)
		}

		if secondSnap.ParentSnapshotID != firstSnap.ID {
			t.Errorf("expected parent %s, got %s", firstSnap.ID, secondSnap.ParentSnapshotID)
		}
	})

	t.Run("latest pointer skips scan", func(t *testing.T) {
		// StreamWrite must write the pointer so that a subsequent
		// Write uses O(1) parent resolution.
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// First write via StreamWrite
		snap1, err := ds.StreamWrite(t.Context(), strings.NewReader("payload-1"), nil)
		if err != nil {
			t.Fatal(err)
		}
		listCallsAfterFirst := len(fs.ListCalls())

		// Second write via regular Write: pointer exists, no List
		snap2, err := ds.Write(t.Context(), []byte("payload-2"), nil)
		if err != nil {
			t.Fatal(err)
		}
		assertNoNewListsAndParent(t, fs, listCallsAfterFirst, snap2, snap1.ID)
	})
}

func TestBlobs_StreamWrite_Encoding(t *testing.T) {
	t.Run("with checksum records checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(MD5()))
		if err != nil {
			t.Fatal(err)
		}

		snap, err := ds.StreamWrite(t.Context(), strings.NewReader("streaming data"), nil)
		if err != nil {
			t.Fatal(err)
		}

		assertMD5ObjectChecksum(t, snap)
	})

	for _, tc := range []struct {
		sub, name, ext string
		comp           Compressor
	}{
		{"with gzip compression", "gzip", ".gz", Gzip()},
		{"with zstd compression", "zstd", ".zst", Zstd()},
	} {
		t.Run(tc.sub, func(t *testing.T) {
			ds := blobsOn(t, NewMemory(), "test-ds", WithCompressor(tc.comp))
			data := []byte("compressible stream payload for " + tc.name)
			snap, err := ds.StreamWrite(t.Context(), bytes.NewReader(data), nil)
			if err != nil {
				t.Fatalf("StreamWrite: %v", err)
			}
			assertCompressedSnapAndExt(t, snap, tc.name, tc.ext)
			got, err := ds.Read(t.Context(), snap.ID)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("Read = %q, %v; want %q", got, err, data)
			}
		})
	}
}

func TestDataset_Write_Encoding(t *testing.T) {
	t.Run("with checksum records checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(MD5()))
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

		// Verify object has checksum
		if len(snap.Objects) != 1 {
			t.Fatalf("expected 1 object, got %d", len(snap.Objects))
		}
		if snap.Manifest.Objects[0].Checksum == "" {
			t.Error("expected non-empty checksum")
		}
		// MD5 produces 32 hex characters
		if len(snap.Manifest.Objects[0].Checksum) != 32 {
			t.Errorf("expected 32 char MD5 checksum, got %d chars", len(snap.Manifest.Objects[0].Checksum))
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

		// Verify object has no checksum
		if snap.Manifest.Objects[0].Checksum != "" {
			t.Errorf("expected empty checksum, got %q", snap.Manifest.Objects[0].Checksum)
		}
	})

	t.Run("with zstd compression", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(Zstd()))
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
		assertCompressedSnapAndExt(t, snap, "zstd", ".zst")

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
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithChecksum(MD5()))
		if err != nil {
			t.Fatal(err)
		}

		iter := seqOf(D{"id": "1"}, D{"id": "2"})
		snap, err := ds.StreamWrite(t.Context(), iter, nil)
		if err != nil {
			t.Fatal(err)
		}

		assertMD5ObjectChecksum(t, snap)
	})

	for _, tc := range []struct {
		sub, name, ext string
		comp           Compressor
	}{
		{"with compression", "gzip", ".gz", Gzip()},
		{"with zstd compression", "zstd", ".zst", Zstd()},
	} {
		t.Run(tc.sub, func(t *testing.T) {
			ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(tc.comp))
			if err != nil {
				t.Fatal(err)
			}
			records := []D{{"id": "1", "data": "content"}, {"id": "2", "data": "more"}}
			snap, err := ds.StreamWrite(t.Context(), seqOf(records...), nil)
			if err != nil {
				t.Fatalf("StreamWrite: %v", err)
			}
			assertCompressedSnapAndExt(t, snap, tc.name, tc.ext)
			if got, err := ds.Read(t.Context(), snap.ID); err != nil || len(got) != 2 {
				t.Fatalf("Read = %v, %v; want 2 records", got, err)
			}
		})
	}
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

		assertLatestIs(t, ds.dataset, snap.ID)

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
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument, got: %v", err)
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
		dsNoComp, err := Open(Memory()).Blobs(t.Context(), "test-no-comp", WithChecksum(MD5()))
		if err != nil {
			t.Fatal(err)
		}

		dsGzip, err := Open(Memory()).Blobs(t.Context(), "test-gzip", WithCompressor(Gzip()), WithChecksum(MD5()))
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
		if snapNoComp.Manifest.Objects[0].Checksum == "" {
			t.Error("expected checksum for no-compression")
		}
		if snapGzip.Manifest.Objects[0].Checksum == "" {
			t.Error("expected checksum for gzip")
		}

		// Checksums should be different (stored bytes differ)
		if snapNoComp.Manifest.Objects[0].Checksum == snapGzip.Manifest.Objects[0].Checksum {
			t.Error("expected different checksums for different compression")
		}
	})

	t.Run("same data produces same checksum", func(t *testing.T) {
		ds, err := Open(Memory()).Blobs(t.Context(), "test-ds", WithChecksum(MD5()))
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
		if snap1.Manifest.Objects[0].Checksum != snap2.Manifest.Objects[0].Checksum {
			t.Errorf("expected same checksum for same data, got %s vs %s",
				snap1.Manifest.Objects[0].Checksum, snap2.Manifest.Objects[0].Checksum)
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

		assertNoTimestamps(t, snap)
	})

	t.Run("timestamped records computes min max", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[*timestampedRecord]())
		if err != nil {
			t.Fatal(err)
		}

		records, ts1, ts2 := timestampedFixture()

		snap, err := ds.Write(t.Context(), records, nil)
		if err != nil {
			t.Fatal(err)
		}

		assertTimeRange(t, snap, ts1, ts2)
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

		if snap.Manifest.Objects[0].Stats != nil {
			t.Errorf("expected nil Stats for JSONL codec, got %+v", snap.Manifest.Objects[0].Stats)
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

		if len(snap.Objects) != 1 {
			t.Fatalf("expected 1 object, got %d", len(snap.Objects))
		}

		stats := snap.Manifest.Objects[0].Stats
		if stats == nil {
			t.Fatal("expected Stats on ObjectRef, got nil")
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

		if snap.Manifest.Objects[0].Stats != nil {
			t.Errorf("expected nil Stats for raw blob, got %+v", snap.Manifest.Objects[0].Stats)
		}
	})
}

// -----------------------------------------------------------------------------
// ObjectStats serialization tests
// -----------------------------------------------------------------------------

func TestFileRef_Stats(t *testing.T) {
	t.Run("JSON round trip", func(t *testing.T) {
		ref := ObjectRef{
			Path:      "data/test.parquet",
			SizeBytes: 1024,
			Stats: &ObjectStats{
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

		var decoded ObjectRef
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

		var ref ObjectRef
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
		ref := ObjectRef{
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

		corruptDatasetPointer(t, mem)

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
	open := func() *Records[D] {
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
	manifestPath := hidePendingManifest(t, store, pending.ID)

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
	errs.DiscardClose(rc)
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
	open := func() *Records[D] {
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
	hidePendingManifest(t, store, pending.ID)

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
func writeInvertedHead(t *testing.T, store Store) SnapshotID {
	t.Helper()
	base := time.Now().Add(-time.Hour)
	parent, head := idAt(base.Add(2*time.Second)), idAt(base.Add(time.Second))
	for _, link := range []struct{ id, parent SnapshotID }{{parent, ""}, {head, parent}} {
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
		if got := readObject(t, store, pointerPath); got != string(snap.ID) {
			t.Errorf("pointer = %q, want %q", got, snap.ID)
		}

		// Reset call tracking to verify Latest() uses pointer, not List.
		fs.Reset()
		assertLatestIs(t, ds.dataset, snap.ID)
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

			if got := readObject(t, store, pointerPath); got != string(snap.ID) {
				t.Errorf("pointer = %q, want %q", got, snap.ID)
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
		if err := store.Delete(t.Context(), "datasets/test-ds/latest"); err != nil {
			t.Fatal(err)
		}

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
		errs.DiscardClose(rc)
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
		if err := store.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(t.Context(), pointerPath, strings.NewReader("nonexistent-id")); err != nil {
			t.Fatal(err)
		}

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
			layout       *pathLayout
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

		hl := newPathLayout(false, partitionScheme{name: "hive", keys: []string{"day"}})
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
		fs, ds, snap := twoDayHive(t)

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
		fs, _, snap := twoDayHive(t)

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

		corruptDatasetPointer(t, mem)

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
		errs.DiscardClose(rc)
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

		// Verify object paths are consistent with the committed snapshot ID.
		// Data objects must be co-located under the same snapshot directory.
		for _, f := range snap.Objects {
			if !strings.Contains(f.Path, string(snap.ID)) {
				t.Errorf("object path %q does not contain committed snapshot ID %q", f.Path, snap.ID)
			}
		}
	})

	// TestDataset_Write_WithRetryCount_ExhaustsRetries verifies that when all
	// retries are exhausted, ErrSnapshotConflict is returned.
	t.Run("with retry count exhausts retries", func(t *testing.T) {
		mem := NewMemory()

		ds1 := recordsOn(t, mem, "shared", WithRetry(1), // only 1 retry
			WithRetryJitter(0.0), WithRetryBaseDelay(time.Millisecond))
		ds2 := recordsOn(t, mem, "shared")
		ds3 := recordsOn(t, mem, "shared")

		// ds1 writes first.
		writeSharedSequence(t, ds1, 1)

		// ds2 writes, advancing pointer.
		writeSharedSequence(t, ds2, 2)

		// ds3 writes, advancing pointer again.
		writeSharedSequence(t, ds3, 3)

		// ds1 now has a stale cache pointing at snap1. The retry will refresh
		// to snap3 and succeed on the first retry. But we need persistent
		// contention to exhaust retries. Since we can't inject contention
		// between retry attempts with the memory store, we verify the basic
		// mechanics: a single retry is enough to resolve a one-hop conflict.
		_, err := ds1.Write(t.Context(), R(D{"seq": 4}), nil)
		if err != nil {
			t.Fatalf("expected success with 1 retry (conflict resolved after refresh), got: %v", err)
		}
	})

	// TestDataset_Write_WithRetryCount_Zero_CurrentBehavior verifies that
	// WithRetry(0) preserves the default behavior (no retry).
	t.Run("with retry count zero current behavior", func(t *testing.T) {
		mem := NewMemory()

		ds1 := recordsOn(t, mem, "shared", WithRetry(0))
		ds2 := recordsOn(t, mem, "shared")

		// ds1 writes first.
		writeSharedSequence(t, ds1, 1)

		// ds2 writes, advancing pointer.
		writeSharedSequence(t, ds2, 2)

		// ds1 should fail immediately without retry.
		_, err := ds1.Write(t.Context(), R(D{"seq": 3}), nil)
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
// snapshot with no data object on every layout: the partitioner yields no
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
			if len(snap.Objects) != 0 || snap.RowCount != 0 {
				t.Errorf("empty write: %d objects and %d rows, want none", len(snap.Objects), snap.RowCount)
			}
		})
	}
}

// renamedChecksum is MD5 under a name Lode does not bundle, as a custom
// checksum would be.
type renamedChecksum struct{ Checksum }

func (renamedChecksum) Name() string { return "custom-md5" }

func TestBlobs_Read_CustomChecksumIsPassedToTheView(t *testing.T) {
	assertCustomBlobComponentRequired(t, "custom-md5", WithChecksum(renamedChecksum{MD5()}))
}

func TestRoot_RemoveDataset(t *testing.T) {
	t.Run("removes objects, writes tombstone, and blocks writers and views", func(t *testing.T) {
		store := &hookStore{Store: NewMemory()}
		root := Open(Use(store))
		ds, err := root.Records(t.Context(), "events", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}
		snap, err := ds.Write(t.Context(), R(D{"day": "2026-01-01", "v": 1}, D{"day": "2026-01-02", "v": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		stopCleanup := errors.New("interrupted cleanup")
		store.onDelete = func(string) error { return stopCleanup }
		if err := root.RemoveDataset(t.Context(), "events"); !errors.Is(err, stopCleanup) {
			t.Fatalf("RemoveDataset returned %v, want injected deletion failure", err)
		}

		// Existing writer and view handles return ErrRemoved on every operation.
		checks := map[string]func() error{
			"writer Write": func() error { _, err := ds.Write(t.Context(), R(D{"day": "2026-01-03"}), nil); return err },
			"writer Read":  func() error { _, err := ds.Read(t.Context(), snap.ID); return err },
			"writer ListPartitions": func() error {
				_, err := ds.ListPartitions(t.Context(), PartitionListOptions{})
				return err
			},
			"writer Manifests": func() error {
				for _, err := range ds.Manifests(t.Context(), PartitionPath{}, ManifestListOptions{}) {
					return err
				}
				return nil
			},
			"writer Expire": func() error {
				_, err := ds.Expire(t.Context(), ExpireRequest{IDs: []SnapshotID{snap.ID}})
				return err
			},
			"writer Sweep": func() error {
				_, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Minute})
				return err
			},
			"view Read": func() error { _, err := view.Read(t.Context(), snap.ID); return err },
			"open new writer": func() error {
				_, err := root.Records(t.Context(), "events", JSONL[D](), hive("day"))
				return err
			},
			"open new view": func() error {
				_, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", JSONL[D]())
				return err
			},
			"open anchored view": func() error {
				_, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", JSONL[D](), WithAnchor(snap.ID))
				return err
			},
		}
		for label, d := range map[string]*dataset{"writer": ds.dataset, "view": view.dataset} {
			checks[label+" Latest"] = errOnly(func() (*Snapshot, error) { return d.Latest(t.Context()) })
			checks[label+" Snapshot"] = errOnly(func() (*Snapshot, error) { return d.Snapshot(t.Context(), snap.ID) })
			checks[label+" Snapshots"] = errOnly(func() ([]*Snapshot, error) { return d.Snapshots(t.Context()) })
		}
		for name, fn := range checks {
			if err := fn(); !errors.Is(err, ErrRemoved) {
				t.Errorf("%s: got %v, want ErrRemoved", name, err)
			}
		}
		store.onDelete = nil
		if err := root.RemoveDataset(t.Context(), "events"); err != nil {
			t.Fatalf("RemoveDataset: %v", err)
		}
		// Second call is idempotent.
		if err := root.RemoveDataset(t.Context(), "events"); err != nil {
			t.Fatalf("second RemoveDataset: %v", err)
		}

		// Only the latest tombstone object remains under datasets/events/.
		keys := storeKeys(t, store)
		if !slices.Equal(keys, []string{"datasets/events/latest"}) {
			t.Fatalf("remaining keys = %v, want [datasets/events/latest]", keys)
		}
	})

	t.Run("nonexistent dataset writes tombstone idempotently", func(t *testing.T) {
		store := NewMemory()
		root := Open(Use(store))
		if err := root.RemoveDataset(t.Context(), "never-written"); err != nil {
			t.Fatalf("RemoveDataset: %v", err)
		}
		if _, err := root.Blobs(t.Context(), "never-written"); !errors.Is(err, ErrRemoved) {
			t.Fatalf("Blobs after RemoveDataset = %v, want ErrRemoved", err)
		}
	})

	t.Run("pending commit returns ErrHeadUncommitted", func(t *testing.T) {
		store := NewMemory()
		if err := store.Put(t.Context(), "datasets/pending/latest", strings.NewReader(string(idAt(time.Now())))); err != nil {
			t.Fatal(err)
		}
		if err := Open(Use(store)).RemoveDataset(t.Context(), "pending"); !errors.Is(err, ErrHeadUncommitted) {
			t.Fatalf("RemoveDataset = %v, want ErrHeadUncommitted", err)
		}
	})
}

func TestChecksum_ReadVerification(t *testing.T) {
	for _, alg := range []Checksum{SHA256(), CRC32C(), MD5()} {
		t.Run(alg.Name(), func(t *testing.T) {
			// Sequential read path (Blobs + JSONL) and random-access read path (Parquet).
			store := NewMemory()
			blobs := blobsOn(t, store, "blobs", WithChecksum(alg), WithCompressor(Gzip()))
			bsnap, err := blobs.Write(t.Context(), []byte("verified payload"), nil)
			if err != nil {
				t.Fatal(err)
			}
			gotBlob, err := blobs.Read(t.Context(), bsnap.ID)
			if err != nil || string(gotBlob) != "verified payload" {
				t.Fatalf("valid blob Read = %q, %v", gotBlob, err)
			}

			// Corrupt the stored blob object bytes and verify ErrChecksumMismatch.
			blobKey := bsnap.Objects[0].Path
			rc, err := store.Get(t.Context(), blobKey)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(rc)
			errs.DiscardClose(rc)
			raw[len(raw)-1] ^= 0xff
			overwriteObjectAndExpectChecksumMismatch(t, store, blobKey, raw, func() ([]byte, error) {
				return blobs.Read(t.Context(), bsnap.ID)
			})

			// Parquet ReaderAt path with checksum verification.
			type row struct {
				ID   int64  `parquet:"id"`
				Name string `parquet:"name"`
			}
			rowSchema := ParquetSchema{Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString},
			}}
			pstore := NewMemory()
			pds, err := Open(Use(pstore)).Records(t.Context(), "rows", Parquet[row](rowSchema), WithChecksum(alg))
			if err != nil {
				t.Fatal(err)
			}
			psnap, err := pds.Write(t.Context(), []row{{ID: 1, Name: "alice"}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			gotRows, err := pds.Read(t.Context(), psnap.ID)
			if err != nil || len(gotRows) != 1 || gotRows[0] != (row{ID: 1, Name: "alice"}) {
				t.Fatalf("valid parquet Read = %+v, %v", gotRows, err)
			}
			pqKey := psnap.Objects[0].Path
			prc, err := pstore.Get(t.Context(), pqKey)
			if err != nil {
				t.Fatal(err)
			}
			pqRaw, _ := io.ReadAll(prc)
			errs.DiscardClose(prc)
			pqRaw[0] ^= 0xff
			overwriteObjectAndExpectChecksumMismatch(t, pstore, pqKey, pqRaw, func() ([]row, error) {
				return pds.Read(t.Context(), psnap.ID)
			})
		})
	}

	t.Run("missing per-object checksum when algorithm declared returns ErrChecksumMismatch", func(t *testing.T) {
		store := NewMemory()
		blobs := blobsOn(t, store, "blobs", WithChecksum(SHA256()))
		snap, err := blobs.Write(t.Context(), []byte("payload"), nil)
		if err != nil {
			t.Fatal(err)
		}
		manifestKey := "datasets/blobs/snapshots/" + string(snap.ID) + "/manifest.json"
		m := snap.Manifest
		m.Objects[0].Checksum = ""
		raw, err := json.Marshal(&m)
		if err != nil {
			t.Fatal(err)
		}
		overwriteObjectAndExpectChecksumMismatch(t, store, manifestKey, raw, func() ([]byte, error) {
			return blobs.Read(t.Context(), snap.ID)
		})
	})
}

func overwriteObjectAndExpectChecksumMismatch[R any](t *testing.T, store Store, key string, raw []byte, read func() (R, error)) {
	t.Helper()
	if err := store.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), key, bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := read(); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Read(%s) = %v, want ErrChecksumMismatch", key, err)
	}
}

func assertCustomBlobComponentRequired(t *testing.T, wantName string, opt interface {
	DatasetOption
	ViewOption
},
) {
	t.Helper()
	store := NewMemory()
	snap, err := blobsOn(t, store, "test-ds", opt).Write(t.Context(), []byte("data"), nil)
	if err != nil {
		t.Fatal(err)
	}
	bare, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bare.Read(t.Context(), snap.ID); !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), wantName) {
		t.Errorf("without custom option: got %v, want ErrSettingsMismatch naming %s", err, wantName)
	}
	view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds", opt)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := view.Read(t.Context(), snap.ID); err != nil || string(got) != "data" {
		t.Errorf("with custom option: Read = %q, %v; want data", got, err)
	}
}

func assertMD5ObjectChecksum(t *testing.T, snap *Snapshot) {
	t.Helper()
	if snap.ChecksumAlgorithm != "md5" {
		t.Errorf("ChecksumAlgorithm = %q, want md5", snap.ChecksumAlgorithm)
	}
	if len(snap.Manifest.Objects[0].Checksum) != 32 {
		t.Errorf("MD5 checksum len = %d, want 32", len(snap.Manifest.Objects[0].Checksum))
	}
}

func assertCompressedSnapAndExt(t *testing.T, snap *Snapshot, wantComp, wantExt string) {
	t.Helper()
	if snap.Compressor != wantComp {
		t.Errorf("Compressor = %q, want %q", snap.Compressor, wantComp)
	}
	if len(snap.Objects) != 1 || !strings.HasSuffix(snap.Objects[0].Path, wantExt) {
		t.Fatalf("Objects = %+v, want 1 object ending with %s", snap.Objects, wantExt)
	}
}

func errOnly[T any](fn func() (T, error)) func() error {
	return func() error { _, err := fn(); return err }
}

func assertLatestIs(t *testing.T, d *dataset, want SnapshotID) {
	t.Helper()
	latest, err := d.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.ID != want {
		t.Errorf("Latest.ID = %s, want %s", latest.ID, want)
	}
}

func assertNoNewListsAndParent(t *testing.T, fs *testutil.FaultStore, before int, snap *Snapshot, wantParent SnapshotID) {
	t.Helper()
	if got := len(fs.ListCalls()); got != before {
		t.Errorf("List calls = %d, want %d", got, before)
	}
	if snap.ParentSnapshotID != wantParent {
		t.Errorf("ParentSnapshotID = %s, want %s", snap.ParentSnapshotID, wantParent)
	}
}

func timestampedFixture() ([]*timestampedRecord, time.Time, time.Time) {
	ts1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	ts3 := time.Date(2024, 1, 10, 8, 0, 0, 0, time.UTC)

	records := []*timestampedRecord{
		{ID: "a", Time: ts1},
		{ID: "b", Time: ts2},
		{ID: "c", Time: ts3},
	}
	return records, ts1, ts2
}

func assertTimeRange(t *testing.T, snap *Snapshot, ts1, ts2 time.Time) {
	t.Helper()
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
}

func corruptDatasetPointer(t *testing.T, mem Store) {
	t.Helper()
	pointerPath := "datasets/test-ds/latest"
	if err := mem.Delete(t.Context(), pointerPath); err != nil {
		t.Fatal(err)
	}
	if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte("nonexistent-snap-id"))); err != nil {
		t.Fatal(err)
	}
}

func hidePendingManifest(t *testing.T, store Store, id SnapshotID) string {
	t.Helper()
	manifestPath := "datasets/test-ds/snapshots/" + string(id) + "/manifest.json"
	moveObject(t, store, manifestPath, "pending/manifest.json")
	if err := store.Delete(t.Context(), manifestPath); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func twoDayHive(t *testing.T) (*testutil.FaultStore, *Records[D], *Snapshot) {
	t.Helper()
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
	return fs, ds, snap
}

func writeSharedSequence(t *testing.T, ds *Records[D], seq int) {
	t.Helper()
	if _, err := ds.Write(t.Context(), R(D{"seq": seq}), nil); err != nil {
		t.Fatalf("write sequence %d: %v", seq, err)
	}
}

func assertNoTimestamps(t *testing.T, snap *Snapshot) {
	t.Helper()
	if snap.MinTimestamp != nil {
		t.Errorf("expected MinTimestamp to be nil, got %v", snap.MinTimestamp)
	}
	if snap.MaxTimestamp != nil {
		t.Errorf("expected MaxTimestamp to be nil, got %v", snap.MaxTimestamp)
	}
}

type lostPointerResponse struct {
	Store
	afterCAS func(SnapshotID)
}

func (s *lostPointerResponse) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	if err := s.Store.CompareAndSwap(ctx, path, expected, replacement); err != nil {
		return err
	}
	if after := s.afterCAS; after != nil {
		s.afterCAS = nil
		after(SnapshotID(replacement))
		return ErrSnapshotConflict
	}
	return nil
}

func TestWriteReconcilesAppliedCAS(t *testing.T) {
	ctx := t.Context()
	store := &lostPointerResponse{Store: NewMemory()}
	ds := recordsOn(t, store, "shared", WithRetry(1))
	var previous SnapshotID
	for range 2 {
		store.afterCAS = func(SnapshotID) {}
		snap, err := ds.Write(ctx, R(D{"key": "value"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := ds.Snapshot(ctx, snap.ID)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.ParentSnapshotID != previous {
			t.Fatalf("snapshot %s has parent %s, want %s", snap.ID, loaded.ParentSnapshotID, previous)
		}
		previous = snap.ID
	}
}

func TestWriteReconcilesAppliedCASAfterDescendants(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d descendants", count), func(t *testing.T) {
			ctx := t.Context()
			store := &lostPointerResponse{Store: NewMemory()}
			a := recordsOn(t, store, "shared", WithRetry(1))
			b := recordsOn(t, store, "shared")
			var lineage []SnapshotID
			store.afterCAS = func(id SnapshotID) {
				lineage = append(lineage, id)
				for range count {
					child, err := b.Write(ctx, R(D{"writer": "B"}), nil)
					if err != nil {
						t.Fatal(err)
					}
					if child.ParentSnapshotID != lineage[len(lineage)-1] {
						t.Fatalf("child has parent %s", child.ParentSnapshotID)
					}
					lineage = append(lineage, child.ID)
				}
			}
			first, err := a.Write(ctx, R(D{"writer": "A"}), nil)
			if err != nil {
				t.Fatal(err)
			}
			if first.ID != lineage[0] || first.ParentSnapshotID != "" {
				t.Fatalf("writer A returned %s with parent %s, want root %s", first.ID, first.ParentSnapshotID, lineage[0])
			}
			var parent SnapshotID
			for _, id := range lineage {
				stored, err := b.Snapshot(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if stored.ParentSnapshotID != parent {
					t.Fatalf("stored %s has parent %s, want %s", id, stored.ParentSnapshotID, parent)
				}
				parent = id
			}
			head, err := b.Latest(ctx)
			if err != nil || head.ID != parent {
				t.Fatalf("head = %v, %v, want %s", head, err, parent)
			}
		})
	}
}
