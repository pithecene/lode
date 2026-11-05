package volume

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// readerAtFixture is a volume of 30 bytes whose snapshot commits [0, 10)
// and [10, 20), over a store that records the keys each call reaches.
type readerAtFixture struct {
	store *testutil.KeyRecorder
	vol   Volume
	snap  *Snapshot
}

func newReaderAtFixture(t *testing.T) readerAtFixture {
	t.Helper()
	store := testutil.NewKeyRecorder(lode.NewMemory())
	vol := newTestVolume(t, "vol", lode.Use[lode.RangeStore](store), 30)
	a := stageBlock(t, vol, 0, []byte("AAAAAAAAAA"))
	b := stageBlock(t, vol, 10, []byte("BBBBBBBBBB"))
	snap := commitBlocks(t, vol, []BlockRef{a, b}, Metadata{})
	return readerAtFixture{store: store, vol: vol, snap: snap}
}

// ReaderAt loads the manifest once; each read then fetches block data only,
// and returns what ReadAt returns for the same range.
func TestReaderAt_ReadsBlockDataOnly(t *testing.T) {
	t.Parallel()
	f := newReaderAtFixture(t)
	var r SnapshotReaderAt
	var err error
	opened := f.store.Recorded(func() { r, err = f.vol.ReaderAt(t.Context(), f.snap.ID) })
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) != 1 || !strings.HasSuffix(opened[0], "manifest.json") {
		t.Fatalf("ReaderAt reached %v, want the manifest alone", opened)
	}
	cases := []struct {
		name        string
		off, length int64
		want        string
	}{
		{name: "within a block", off: 2, length: 3, want: "AAA"},
		{name: "spanning blocks", off: 5, length: 10, want: "AAAAABBBBB"},
		{name: "one byte", off: 19, length: 1, want: "B"},
	}
	for _, tc := range cases {
		p := make([]byte, tc.length)
		var n int
		keys := f.store.Recorded(func() { n, err = r.ReadAt(p, tc.off) })
		if err != nil || n != len(tc.want) || string(p) != tc.want {
			t.Errorf("%s: ReadAt = %d %q, %v; want %q", tc.name, n, p, err, tc.want)
		}
		for _, k := range keys {
			if strings.HasSuffix(k, "manifest.json") || strings.HasSuffix(k, "/latest") {
				t.Errorf("%s: ReadAt reached %s, want block data only", tc.name, k)
			}
		}
		if direct, err := f.vol.ReadAt(t.Context(), f.snap.ID, tc.off, tc.length); err != nil || !bytes.Equal(direct, p) {
			t.Errorf("%s: Reader.ReadAt = %q, %v; want %q", tc.name, direct, err, p)
		}
	}
}

// The reader follows io.ReaderAt at the end of the volume, and a range
// that is not fully committed returns ErrRangeMissing and no bytes.
func TestReaderAt_Bounds(t *testing.T) {
	t.Parallel()
	f := newReaderAtFixture(t)
	r, err := f.vol.ReaderAt(t.Context(), f.snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		off     int64
		size    int
		n       int
		wantErr error
	}{
		{name: "past the end", off: 30, size: 4, n: 0, wantErr: io.EOF},
		{name: "uncommitted tail", off: 15, size: 10, n: 0, wantErr: ErrRangeMissing},
		{name: "empty buffer", off: 0, size: 0, n: 0},
		{name: "negative offset", off: -1, size: 1, n: 0, wantErr: lode.ErrInvalidArgument},
	}
	for _, tc := range cases {
		n, err := r.ReadAt(make([]byte, tc.size), tc.off)
		if n != tc.n || !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
			t.Errorf("%s: ReadAt = %d, %v; want %d, %v", tc.name, n, err, tc.n, tc.wantErr)
		}
	}
}

// A read that crosses the end of the volume returns the bytes before the
// end with io.EOF.
func TestReaderAt_ReadAcrossTheEnd(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	vol := newTestVolume(t, "vol", lode.Use[lode.RangeStore](store), 10)
	snap := commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("0123456789"))}, Metadata{})
	r, err := vol.ReaderAt(t.Context(), snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 8)
	n, err := r.ReadAt(p, 6)
	if n != 4 || !errors.Is(err, io.EOF) || string(p[:n]) != "6789" {
		t.Fatalf("ReadAt across the end = %d %q, %v; want 4 \"6789\", io.EOF", n, p[:n], err)
	}
	// A read that ends exactly at the end fills p; io.ReaderAt allows nil
	// there, and the reader returns nil.
	exact := make([]byte, 4)
	if n, err := r.ReadAt(exact, 6); n != 4 || err != nil || string(exact) != "6789" {
		t.Fatalf("ReadAt to the end = %d %q, %v; want 4 \"6789\", nil", n, exact, err)
	}
	if got, err := io.ReadAll(io.NewSectionReader(r, 0, 10)); err != nil || string(got) != "0123456789" {
		t.Fatalf("a section reader over the reader = %q, %v", got, err)
	}
}

// Blocks returns a copy: a change to it
// changes neither the reader's reads nor a later copy.
func TestReaderAt_BlocksAreACopy(t *testing.T) {
	t.Parallel()
	f := newReaderAtFixture(t)
	r, err := f.vol.ReaderAt(t.Context(), f.snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	first := r.Blocks()
	first[0].Path = "elsewhere"
	second := r.Blocks()
	if len(second) != 2 || second[0].Path == "elsewhere" {
		t.Fatalf("blocks after a change to a copy: %v", second)
	}
	p := make([]byte, 20)
	if n, err := r.ReadAt(p, 0); err != nil || n != 20 {
		t.Fatalf("ReadAt after a change to a copy = %d, %v", n, err)
	}
}

// The reader serves concurrent reads, as io.ReaderAt requires; the race
// detector checks that they share no unguarded state.
func TestReaderAt_ConcurrentReads(t *testing.T) {
	t.Parallel()
	f := newReaderAtFixture(t)
	r, err := f.vol.ReaderAt(t.Context(), f.snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	const readers = 8
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for i := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := make([]byte, 10)
			off := int64(i%2) * 10
			if _, err := r.ReadAt(p, off); err != nil {
				errs <- err
				return
			}
			_ = r.Blocks()
			errs <- nil
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

// A read of a range with an uncommitted gap fails before it allocates the
// range: a sparse volume may be far larger than its committed bytes. The
// test measures the heap, so it does not run in parallel.
//
//nolint:paralleltest // TotalAlloc counts every goroutine's allocations
func TestReadAt_GapFailsBeforeAllocating(t *testing.T) {
	const size = 64 << 20
	vol := newTestVolume(t, "vol", lode.Use[lode.RangeStore](lode.NewMemory()), size)
	snap := commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("A"))}, Metadata{})
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := vol.ReadAt(t.Context(), snap.ID, 0, size)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrRangeMissing) {
		t.Fatalf("ReadAt over a gap: %v, want ErrRangeMissing", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew >= size/2 {
		t.Fatalf("ReadAt over a gap allocated %d bytes, want far fewer than the %d-byte range", grew, size)
	}
}

// ReaderAt of an absent snapshot fails as Snapshot does.
func TestReaderAt_AbsentSnapshot(t *testing.T) {
	t.Parallel()
	f := newReaderAtFixture(t)
	if _, err := f.vol.ReaderAt(t.Context(), "absent"); !errors.Is(err, lode.ErrNotFound) {
		t.Fatalf("ReaderAt of an absent snapshot: %v, want ErrNotFound", err)
	}
}
