package lode

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// latencyStore wraps a Store and injects artificial latency on List and Get
// calls. This simulates remote store behavior (S3/R2) where each API call
// costs 50-100ms, making O(n) store scans per write visible as wall-clock
// degradation.
type latencyStore struct {
	inner   RangeStore
	latency time.Duration
}

func (s *latencyStore) Put(ctx context.Context, path string, r io.Reader) error {
	return s.inner.Put(ctx, path, r)
}

func (s *latencyStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	time.Sleep(s.latency)
	return s.inner.Get(ctx, path)
}

func (s *latencyStore) Exists(ctx context.Context, path string) (bool, error) {
	return s.inner.Exists(ctx, path)
}

func (s *latencyStore) List(ctx context.Context, prefix string) ([]string, error) {
	time.Sleep(s.latency)
	return s.inner.List(ctx, prefix)
}

func (s *latencyStore) Delete(ctx context.Context, path string) error {
	return s.inner.Delete(ctx, path)
}

func (s *latencyStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return s.inner.ReadRange(ctx, path, offset, length)
}

func (s *latencyStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	return s.inner.ReaderAt(ctx, path)
}

// CompareAndSwap forwards to the inner store's ConditionalWriter implementation.
// This ensures benchmarks exercise the CAS commit path, not the Delete+Put fallback.
func (s *latencyStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	return s.inner.(ConditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}

// BenchmarkDataset_SequentialWrites measures the cost of N sequential writes.
//
// With the persistent latest pointer (issues #118/#119), writes 2..N resolve
// the parent in O(1) via a single Get. The first write to a new dataset falls
// back to scan (List), but subsequent writes read the pointer directly.
//
// The 1ms simulated latency makes quadratic regression obvious: 20 writes would
// take ~400ms without pointer (20×20×1ms) vs ~20ms with pointer.
func BenchmarkDataset_SequentialWrites(b *testing.B) {
	const writeCount = 20

	ls := &latencyStore{
		inner:   NewMemory(),
		latency: 1 * time.Millisecond,
	}
	factory := Use[Store](ls)

	ds, err := Open(factory).Records(b.Context(), "bench-ds", JSONL[D]())
	if err != nil {
		b.Fatal(err)
	}

	data := R(D{"key": "value"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < writeCount; j++ {
			if _, err := ds.Write(b.Context(), data, nil); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkDataset_SequentialWrites_StoreCallCount verifies that store.List
// calls remain O(1) regardless of snapshot count. With the persistent latest
// pointer, only the cold-start write (no pointer yet) triggers a List call.
// All subsequent writes resolve the parent via the pointer (Get only).
func BenchmarkDataset_SequentialWrites_StoreCallCount(b *testing.B) {
	fs := testutil.NewFaultStore(NewMemory())
	factory := Use(fs)

	ds, err := Open(factory).Records(b.Context(), "bench-ds", JSONL[D]())
	if err != nil {
		b.Fatal(err)
	}

	data := R(D{"key": "value"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fs.Reset()

		// First write of iteration (may call List on very first iteration
		// if no pointer exists yet; pointer-based on subsequent iterations).
		if _, err := ds.Write(b.Context(), data, nil); err != nil {
			b.Fatal(err)
		}
		firstWriteListCalls := len(fs.ListCalls())

		// Subsequent writes (should NOT call List — pointer always exists)
		for j := 1; j < 20; j++ {
			if _, err := ds.Write(b.Context(), data, nil); err != nil {
				b.Fatal(err)
			}
		}

		totalListCalls := len(fs.ListCalls())
		if totalListCalls != firstWriteListCalls {
			b.Fatalf("expected %d List calls (first write only), got %d after 20 writes",
				firstWriteListCalls, totalListCalls)
		}
	}
}

// partitionReadBench writes snapshots of one record each into one Hive
// partition over a store with simulated remote latency, and opens a view of
// them, as a reader of a run's partition finds it.
func partitionReadBench(b *testing.B, snapshots int) (SnapshotReader, PartitionPath, *testutil.FaultStore) {
	b.Helper()
	fs := testutil.NewFaultStore(NewMemory())
	ds, err := Open(Use[Store](fs)).Records(b.Context(), "bench-ds", JSONL[D](), hive("day"))
	if err != nil {
		b.Fatal(err)
	}
	for range snapshots {
		if _, err := ds.Write(b.Context(), R(D{"day": "a"}), nil); err != nil {
			b.Fatal(err)
		}
	}
	view, err := OpenReadOnly(Use[Store](&latencyStore{inner: fs, latency: time.Millisecond})).Records(b.Context(), "bench-ds", JSONL[D]())
	if err != nil {
		b.Fatal(err)
	}
	day, err := ParsePartitionPath("day=a")
	if err != nil {
		b.Fatal(err)
	}
	return view, day, fs
}

// readFiles opens and drains each file a manifest names.
func readFiles(b *testing.B, r SnapshotReader, ref ManifestRef, m *Manifest) {
	b.Helper()
	for _, f := range m.Files {
		rc, err := r.OpenObject(b.Context(), ObjectRef{Manifest: ref, Path: f.Path})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatal(err)
		}
		if err := rc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPartitionRead reads every snapshot of a partition of 72, the
// artifact count of a day archive retained in chunks, with 1ms of latency
// per Get and List: by ListManifests then GetManifest, and by Manifests.
// It reports the store Gets each read makes.
func BenchmarkPartitionRead(b *testing.B) {
	const snapshots = 72
	reads := []struct {
		name string
		read func(b *testing.B, r SnapshotReader, day PartitionPath)
	}{
		{"ListThenGet", func(b *testing.B, r SnapshotReader, day PartitionPath) {
			refs, err := r.ListManifests(b.Context(), day, ManifestListOptions{})
			if err != nil {
				b.Fatal(err)
			}
			for _, ref := range refs {
				m, err := r.GetManifest(b.Context(), ref)
				if err != nil {
					b.Fatal(err)
				}
				readFiles(b, r, ref, m)
			}
		}},
		{"Manifests", func(b *testing.B, r SnapshotReader, day PartitionPath) {
			for loaded, err := range r.Manifests(b.Context(), day, ManifestListOptions{}) {
				if err != nil {
					b.Fatal(err)
				}
				readFiles(b, r, loaded.Ref, loaded.Manifest)
			}
		}},
	}
	for _, read := range reads {
		b.Run(read.name, func(b *testing.B) {
			view, day, fs := partitionReadBench(b, snapshots)
			gets := 0
			b.ResetTimer()
			for b.Loop() {
				fs.Reset()
				read.read(b, view, day)
				gets += len(fs.GetCalls())
			}
			b.ReportMetric(float64(gets)/float64(b.N), "gets/op")
		})
	}
}
