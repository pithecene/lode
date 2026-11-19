package lode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// Deterministic Streaming Failure Tests
// -----------------------------------------------------------------------------

// TestStreamWrite_CommitFailure covers which failures remove a stream's data
// object: only those that leave no manifest behind.
func TestStreamWrite_CommitFailure(t *testing.T) {
	t.Run("pointer error removes data", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		fs.SetCASError(testutil.ErrInjectedPut)
		if _, err := ds.StreamWrite(t.Context(), strings.NewReader("data"), nil); err == nil {
			t.Fatal("expected StreamWrite to fail on the pointer")
		}
		if !slices.ContainsFunc(fs.DeleteCalls(), func(p string) bool { return strings.Contains(p, "/data/") }) {
			t.Errorf("data object was not removed after the pointer failed: %v", fs.DeleteCalls())
		}
	})

	t.Run("incomplete ancestry preserves the staged manifest and blob", func(t *testing.T) {
		ctx := t.Context()
		store := &lostPointerResponse{Store: NewMemory()}
		ds := blobsOn(t, store, "events", WithRetry(1))
		var attempted SnapshotID
		rival := history.NewID()
		pointer := ds.layout.latestPointerPath(ds.id)
		store.afterCAS = func(id SnapshotID) {
			attempted = id
			if err := store.Store.CompareAndSwap(ctx, pointer, string(id), rival); err != nil {
				t.Fatal(err)
			}
		}
		_, err := ds.StreamWrite(ctx, strings.NewReader("payload"), nil)
		if !errors.Is(err, history.ErrManifestUncertain) || !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("StreamWrite = %v, want an uncertain CAS conflict", err)
		}
		if got := readObject(t, store, pointer); got != rival {
			t.Fatalf("pointer = %q, want %q", got, rival)
		}
		staged := readManifest(t, store, ds.layout.stagedManifestPath(ds.id, attempted))
		if len(staged.Objects) != 1 || readObject(t, store, staged.Objects[0].Path) != "payload" {
			t.Fatal("missing staged blob after an uncertain commit")
		}
	})

	t.Run("lost manifest response keeps the snapshot readable", func(t *testing.T) {
		store := lostManifestResponse{Store: NewMemory()}
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds.StreamWrite(t.Context(), strings.NewReader("payload"), nil); err == nil {
			t.Fatal("expected StreamWrite to report the lost manifest response")
		}
		head, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("the snapshot whose manifest was stored is not visible: %v", err)
		}
		if _, err := ds.Read(t.Context(), head.ID); err != nil {
			t.Fatalf("visible snapshot %s is not readable: %v", head.ID, err)
		}
	})
}

func TestStreamWrite_Cleanup(t *testing.T) {
	t.Run("manifest put error retains data", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		fs.SetPutError(testutil.ErrInjectedPut, "manifest")
		if _, err := ds.StreamWrite(t.Context(), strings.NewReader("partial data"), nil); err == nil {
			t.Fatal("expected StreamWrite to fail with manifest Put error")
		}

		fs.SetPutError(nil)
		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after failed commit, got: %v", err)
		}
		assertDataRetained(t, fs)
	})

	t.Run("reader error no manifest cleanup attempted", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := ds.StreamWrite(t.Context(), newFailingReader("will be aborted"), nil); !errors.Is(err, errReaderBoom) {
			t.Fatalf("StreamWrite with failing reader = %v, want %v", err, errReaderBoom)
		}

		assertUncommittedCleanStore(t, ds.dataset, fs, inner)
	})

	t.Run("cleanup error ignored", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		fs.SetCASError(testutil.ErrInjectedPut)
		fs.SetDeleteError(testutil.ErrInjectedDelete)

		_, err = ds.StreamWrite(t.Context(), strings.NewReader("data"), nil)
		if !errors.Is(err, testutil.ErrInjectedPut) {
			t.Fatalf("StreamWrite = %v, want pointer error %v (Delete error ignored)", err, testutil.ErrInjectedPut)
		}
	})
}

func assertNoDataObject(t *testing.T, fs *testutil.FaultStore, inner Store) {
	t.Helper()
	for _, p := range fs.PutCalls() {
		if !strings.Contains(p, "/data/") {
			continue
		}
		exists, err := inner.Exists(t.Context(), p)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("failed stream left a data object at %s", p)
		}
	}
}

// -----------------------------------------------------------------------------
// StreamWriteRecords Fault Tests
// -----------------------------------------------------------------------------

func TestStreamWriteRecords_Cleanup(t *testing.T) {
	t.Run("iterator error no manifest cleanup attempted", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		seq := failAfterSeq([]D{{"id": "1"}, {"id": "2"}}, 1, errors.New("iterator failure"))
		if _, err := ds.StreamWrite(t.Context(), seq, nil); err == nil {
			t.Fatal("expected error from failing iterator")
		}

		assertUncommittedCleanStore(t, ds.dataset, fs, inner)
	})

	t.Run("manifest put error no manifest", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		fs.SetPutError(testutil.ErrInjectedPut, "manifest")
		if _, err := ds.StreamWrite(t.Context(), seqOf(D{"id": "1"}, D{"id": "2"}), nil); err == nil {
			t.Fatal("expected error from manifest Put failure")
		}

		fs.SetPutError(nil)
		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after manifest Put error, got: %v", err)
		}
		assertDataRetained(t, fs)
	})
}

// -----------------------------------------------------------------------------
// Context Cancellation & Fast Put Failure Tests
// -----------------------------------------------------------------------------

func TestStreamWrite_PutFailure(t *testing.T) {
	t.Run("blocked put context cancel no manifest", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		casBlock := make(chan struct{})
		pointerCASEntered := make(chan struct{}, 1)
		fs.SetCASBlock(casBlock)
		fs.SetBeforeCAS(func(path string) {
			if strings.Contains(path, "latest") {
				select {
				case pointerCASEntered <- struct{}{}:
				default:
				}
			}
		})

		commitDone := make(chan error, 1)
		go func() {
			_, err := ds.StreamWrite(ctx, strings.NewReader("data"), nil)
			commitDone <- err
		}()

		select {
		case <-pointerCASEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("commit did not reach pointer CAS")
		}

		cancel()
		close(casBlock)

		select {
		case err = <-commitDone:
			if err == nil {
				t.Fatal("expected error from blocked commit with canceled context")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("commit did not complete after unblock")
		}

		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after blocked+canceled Put, got: %v", err)
		}
	})

	t.Run("data put fails fast returns error", func(t *testing.T) {
		for _, comp := range []Compressor{Uncompressed(), Zstd()} {
			fs := testutil.NewFaultStore(NewMemory())
			fs.SetPutError(testutil.ErrInjectedPut, "/data/")

			ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds", WithCompressor(comp))
			if err != nil {
				t.Fatal(err)
			}

			err = awaitOrFail(t, func() error {
				_, werr := ds.StreamWrite(t.Context(), bytes.NewReader(make([]byte, 1<<20)), nil)
				return werr
			})
			if !errors.Is(err, testutil.ErrInjectedPut) {
				t.Fatalf("%s: got %v, want testutil.ErrInjectedPut", comp.Name(), err)
			}

			fs.SetPutError(nil)
			if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
				t.Errorf("%s: expected ErrNoSnapshots after failed stream, got: %v", comp.Name(), err)
			}
		}
	})

	t.Run("buffered compressor stops once put returned", func(t *testing.T) {
		fs, putReturned := newZstdDataPutFaultStore(t, ErrPathExists)
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds", WithCompressor(Zstd()))
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), &endlessReaderAfter{gate: putReturned}, nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
		assertNoDataDelete(t, fs)
	})
}

func TestStreamWrite_ErrorPrecedence(t *testing.T) {
	t.Run("commit put failure precedes compressor close error", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		gate := afterPutPublished(fs)
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds", WithCompressor(closeFailCompressor{Zstd()}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = ds.StreamWrite(t.Context(), &gatedEOFReader{gate: gate}, nil)
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("StreamWrite: got %v, want ErrPathExists", err)
		}

		fs2 := testutil.NewFaultStore(NewMemory())
		ds2, err := Open(Use(fs2)).Blobs(t.Context(), "test-ds", WithCompressor(closeFailCompressor{Zstd()}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds2.StreamWrite(t.Context(), strings.NewReader("x"), nil); !errors.Is(err, errCloseBoom) {
			t.Fatalf("StreamWrite without Put failure: got %v, want errCloseBoom", err)
		}
	})

	t.Run("compressor write error stops stream", func(t *testing.T) {
		boom := errors.New("compressor refused")
		comp := &writeFailCompressor{Compressor: Zstd(), err: boom}
		ds, err := Open(Use(NewMemory())).Blobs(t.Context(), "sticky", WithCompressor(comp))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds.StreamWrite(t.Context(), strings.NewReader("ab"), nil); !errors.Is(err, boom) {
			t.Fatalf("StreamWrite: got %v, want %v", err, boom)
		}
		if comp.writes != 1 {
			t.Errorf("compressor saw %d writes, want 1", comp.writes)
		}
	})
}

func awaitOrFail(t *testing.T, op func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- op() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("stream write blocked after fast Put failure")
		return nil
	}
}

func TestStreamWriteRecords_PutFailure(t *testing.T) {
	t.Run("data put fails fast returns error", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(testutil.ErrInjectedPut, "/data/")

		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), seqOf(D{"id": "1"}, D{"id": "2"}), nil)
			return werr
		})
		if !errors.Is(err, testutil.ErrInjectedPut) {
			t.Fatalf("got %v, want testutil.ErrInjectedPut", err)
		}

		fs.SetPutError(nil)
		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after failed stream, got: %v", err)
		}
	})

	t.Run("buffered compressor stops once put returned", func(t *testing.T) {
		fs, putReturned := newZstdDataPutFaultStore(t, ErrPathExists)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(Zstd()))
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), endlessAfter(putReturned), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
		assertNoDataDelete(t, fs)
	})

	t.Run("put failure takes precedence over iterator error", func(t *testing.T) {
		fs, gate, ds := openGatedZstdFaultRecords(t)
		err := awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), oneThenGate(gate, errIterator), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists to take precedence", err)
		}
		assertNoDataDelete(t, fs)
	})

	t.Run("put failure during iterator err takes precedence", func(t *testing.T) {
		fs, putReturned := newZstdDataPutFaultStore(t, ErrPathExists)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(Zstd()))
		if err != nil {
			t.Fatal(err)
		}
		gate := make(chan struct{})
		go func() {
			<-putReturned
			time.Sleep(10 * time.Millisecond)
			close(gate)
		}()
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), oneThenGate(gate, errIterator), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists to take precedence", err)
		}
	})

	t.Run("reported when the iterator ends after the failure", func(t *testing.T) {
		_, gate, ds := openGatedZstdFaultRecords(t)
		err := awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), oneThenGate(gate, nil), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
	})
}

func assertDataRetained(t *testing.T, fs *testutil.FaultStore) {
	t.Helper()
	for _, p := range fs.DeleteCalls() {
		if strings.Contains(p, "/data/") {
			t.Fatalf("the data object of a possibly visible snapshot was deleted: %v", fs.DeleteCalls())
		}
	}
}

func assertNoDataDelete(t *testing.T, fs *testutil.FaultStore) {
	t.Helper()
	for _, p := range fs.DeleteCalls() {
		if strings.Contains(p, "/data/") {
			t.Fatalf("cleanup deleted a foreign data object: %v", fs.DeleteCalls())
		}
	}
}

func TestStreamWrite_ForeignObject(t *testing.T) {
	t.Run("put path exists does not delete foreign object", func(t *testing.T) {
		for _, comp := range []Compressor{Uncompressed(), Zstd()} {
			fs := testutil.NewFaultStore(NewMemory())
			fs.SetPutError(ErrPathExists, "/data/")
			ds, err := streamOneMegabyteBlob(t, fs, comp)
			if !errors.Is(err, ErrPathExists) {
				t.Fatalf("%s: expected ErrPathExists, got %v", comp.Name(), err)
			}
			assertNoDataDelete(t, fs)
			if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
				t.Fatalf("%s: expected ErrNoSnapshots, got %v", comp.Name(), err)
			}
		}
	})

	t.Run("read before check store preserves foreign object", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)
		ctx := t.Context()
		fs.SetBeforePut(func(path string) {
			if strings.Contains(path, "/data/") {
				_ = inner.Put(ctx, path, strings.NewReader("foreign"))
			}
		})
		ds, err := Open(Use(fs)).Blobs(ctx, "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(ctx, strings.NewReader("ours"), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
		assertNoDataDelete(t, fs)
		for _, p := range fs.PutCalls() {
			if strings.Contains(p, "/data/") {
				rc, gerr := inner.Get(ctx, p)
				if gerr != nil {
					t.Fatalf("foreign object missing after cleanup: %v", gerr)
				}
				_ = rc.Close()
			}
		}
	})
}

func TestStreamWriteRecords_ForeignObject(t *testing.T) {
	t.Run("put path exists does not delete foreign object", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), seqOf(D{"id": "1"}), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
		assertNoDataDelete(t, fs)
	})

	t.Run("propagated err path exists leaves nothing", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		bad := func(yield func(D, error) bool) {
			if !yield(D{"id": "1"}, nil) {
				return
			}
			yield(nil, ErrPathExists)
		}
		if _, err := ds.StreamWrite(t.Context(), bad, nil); !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
		assertNoDataObject(t, fs, inner)
		assertNoDataDelete(t, fs)
	})
}

func failAfterSeq(records []D, count int, failErr error) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		for i, r := range records {
			if i >= count {
				yield(nil, failErr)
				return
			}
			if !yield(r, nil) {
				return
			}
		}
	}
}

var errIterator = errors.New("iterator failure")

func endlessAfter(gate <-chan struct{}) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		<-gate
		for {
			if !yield(D{"id": "1"}, nil) {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
}

type endlessReaderAfter struct {
	gate <-chan struct{}
	once bool
}

func (r *endlessReaderAfter) Read(p []byte) (int, error) {
	if !r.once {
		r.once = true
		<-r.gate
	}
	time.Sleep(time.Millisecond)
	p[0] = 'x'
	return 1, nil
}

type gatedEOFReader struct {
	gate <-chan struct{}
}

func (r *gatedEOFReader) Read([]byte) (int, error) {
	<-r.gate
	return 0, io.EOF
}

func oneThenGate(gate <-chan struct{}, tail error) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		if !yield(D{"id": "1"}, nil) {
			return
		}
		<-gate
		if tail != nil {
			yield(nil, tail)
		}
	}
}

func afterPutPublished(fs *testutil.FaultStore) <-chan struct{} {
	putReturned := make(chan struct{})
	fs.SetAfterPut(func(path string, _ error) {
		if strings.Contains(path, "/data/") {
			close(putReturned)
		}
	})
	gate := make(chan struct{})
	go func() {
		<-putReturned
		time.Sleep(10 * time.Millisecond)
		close(gate)
	}()
	return gate
}

var errCloseBoom = errors.New("compressor close failure")

type closeFailCompressor struct{ Compressor }

func (c closeFailCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wc, err := c.Compressor.Compress(w)
	if err != nil {
		return nil, err
	}
	return &closeFailWriter{wc}, nil
}

type closeFailWriter struct{ io.WriteCloser }

func (c *closeFailWriter) Close() error {
	_ = c.WriteCloser.Close()
	return errCloseBoom
}

type writeFailCompressor struct {
	Compressor
	writes int
	err    error
}

func (c *writeFailCompressor) Compress(io.Writer) (io.WriteCloser, error) { return c, nil }
func (c *writeFailCompressor) Write([]byte) (int, error)                  { c.writes++; return 0, c.err }
func (c *writeFailCompressor) Close() error                               { return nil }

type lostManifestResponse struct{ Store }

func (s lostManifestResponse) Put(ctx context.Context, path string, r io.Reader) error {
	if err := s.Store.Put(ctx, path, r); err != nil || !strings.HasSuffix(path, "manifest.json") {
		return err
	}
	return errors.New("injected: response lost after the manifest was stored")
}

func (s lostManifestResponse) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	return s.Store.(ConditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}

func newZstdDataPutFaultStore(t *testing.T, putErr error) (*testutil.FaultStore, chan struct{}) {
	t.Helper()
	fs := testutil.NewFaultStore(NewMemory())
	fs.SetPutError(putErr, "/data/")
	putReturned := make(chan struct{})
	fs.SetAfterPut(func(path string, _ error) {
		if strings.Contains(path, "/data/") {
			close(putReturned)
		}
	})
	return fs, putReturned
}

func openGatedZstdFaultRecords(t *testing.T) (*testutil.FaultStore, <-chan struct{}, *Records[D]) {
	t.Helper()
	fs := testutil.NewFaultStore(NewMemory())
	fs.SetPutError(ErrPathExists, "/data/")
	gate := afterPutPublished(fs)
	ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(Zstd()))
	if err != nil {
		t.Fatal(err)
	}
	return fs, gate, ds
}

func streamOneMegabyteBlob(t *testing.T, store Store, comp Compressor) (*Blobs, error) {
	t.Helper()
	ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds", WithCompressor(comp))
	if err != nil {
		t.Fatal(err)
	}
	return ds, awaitOrFail(t, func() error {
		_, werr := ds.StreamWrite(t.Context(), bytes.NewReader(make([]byte, 1<<20)), nil)
		return werr
	})
}

func assertUncommittedCleanStore(t *testing.T, d *dataset, fs *testutil.FaultStore, inner Store) {
	t.Helper()
	if _, err := d.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("Latest = %v, want ErrNoSnapshots", err)
	}
	assertNoDataObject(t, fs, inner)
	assertNoDataDelete(t, fs)
}
