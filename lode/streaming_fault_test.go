package lode

import (
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// Deterministic Streaming Failure Tests
// -----------------------------------------------------------------------------
//
// These tests use the fault-injection store wrapper to verify contract guarantees
// for streaming write failure paths WITHOUT relying on timing or network behavior.
//
// Key guarantees tested:
// - No manifest on failed/canceled stream (snapshot invisibility)
// - Cleanup attempt occurred (best-effort delete)
// - Deterministic assertions without race conditions
//
// These tests give deterministic coverage of failure paths that real adapters
// reach only through timing (docs/maintainers/TEST_MATRIX.md §Streaming Cancellation).

// -----------------------------------------------------------------------------
// StreamWrite Fault Tests
// -----------------------------------------------------------------------------

// TestStreamWrite_CommitFailure covers which failures remove a stream's data
// object: only those that leave no manifest behind.
func TestStreamWrite_CommitFailure(t *testing.T) {
	// A pointer CAS that fails leaves no manifest, so the data object of the
	// failed commit is removed.
	t.Run("pointer error removes data", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		fs.SetCASError(testutil.ErrInjectedPut)
		if _, err := sw.Commit(t.Context()); err == nil {
			t.Fatal("expected Commit to fail on the pointer")
		}
		if !slices.ContainsFunc(fs.DeleteCalls(), func(p string) bool { return strings.Contains(p, "/data/") }) {
			t.Errorf("data object was not removed after the pointer failed: %v", fs.DeleteCalls())
		}
	})

	// A manifest Put whose response is lost stores the manifest, so the
	// snapshot is visible; its data must still be readable.
	t.Run("lost manifest response keeps the snapshot readable", func(t *testing.T) {
		store := lostManifestResponse{Store: NewMemory()}
		ds, err := Open(Use(store)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Write([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Commit(t.Context()); err == nil {
			t.Fatal("expected Commit to report the lost manifest response")
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
	// A manifest Put that reports an error may still have stored the
	// manifest, so the commit keeps its data object; Sweep reclaims it if the
	// commit did fail.
	t.Run("manifest put error retains data", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write some data (data file Put is in progress)
		_, err = sw.Write([]byte("partial data"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Inject Put error ONLY for manifest paths
		// The data file Put is already in progress and won't be affected
		fs.SetPutError(testutil.ErrInjectedPut, "manifest")

		// Commit should fail when writing manifest
		_, err = sw.Commit(t.Context())
		if err == nil {
			t.Fatal("expected Commit to fail with manifest Put error")
		}

		// Clear error to allow Latest to work
		fs.SetPutError(nil)

		// Verify: no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after failed commit, got: %v", err)
		}

		assertDataRetained(t, fs)
	})

	// TestStreamWrite_Abort_NoManifest_CleanupAttempted verifies that Abort leaves
	// no manifest and attempts cleanup.
	t.Run("abort no manifest cleanup attempted", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write data
		_, err = sw.Write([]byte("will be aborted"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Record puts before abort
		putsBefore := len(fs.PutCalls())

		// Abort
		err = sw.Abort(t.Context())
		if err != nil {
			t.Fatalf("Abort failed: %v", err)
		}

		// Verify: no manifest written (no additional Put after abort)
		putsAfter := len(fs.PutCalls())
		if putsAfter > putsBefore {
			t.Errorf("expected no additional Put calls after abort, got %d new calls", putsAfter-putsBefore)
		}

		// Verify: no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after abort, got: %v", err)
		}

		// Verify: the aborted Put left no object behind, without the writer
		// deleting anything (CONTRACT_STORAGE §Put is all-or-nothing).
		assertNoDataObject(t, fs, inner)
		assertNoDataDelete(t, fs)
	})

	// TestStreamWrite_CloseWithoutCommit_NoManifest_CleanupAttempted verifies that
	// Close without Commit behaves as Abort.
	t.Run("close without commit no manifest cleanup attempted", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write data
		_, err = sw.Write([]byte("will be abandoned"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Close without commit
		err = sw.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify: no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after close without commit, got: %v", err)
		}

		// Verify: no object remains and the writer deleted nothing.
		assertNoDataObject(t, fs, inner)
		assertNoDataDelete(t, fs)
	})

	// TestStreamWrite_CleanupErrorIgnored verifies Abort succeeds even when the
	// store's Delete fails: the writer has nothing to delete after a failed Put,
	// so a broken Delete cannot affect the outcome.
	t.Run("cleanup error ignored", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Inject Delete error to verify it's ignored
		fs.SetDeleteError(testutil.ErrInjectedDelete)

		// Abort should succeed even with Delete error
		err = sw.Abort(t.Context())
		if err != nil {
			t.Fatalf("Abort failed despite Delete error: %v", err)
		}

		assertNoDataObject(t, fs, inner)
	})
}

// assertNoDataObject verifies no data object exists in the inner store for
// any data path the stream handed to Put.
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
	// TestStreamWriteRecords_IteratorError_NoManifest_CleanupAttempted verifies that
	// when the iterator returns an error, no manifest is written and cleanup occurs.
	t.Run("iterator error no manifest cleanup attempted", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Create an iterator that fails after yielding some records
		iter := failAfterSeq([]D{{"id": "1"}, {"id": "2"}}, 1, errors.New("iterator failure"))

		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if err == nil {
			t.Fatal("expected error from failing iterator")
		}

		// Verify: no snapshot visible
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after iterator error, got: %v", err)
		}

		// Verify: no object remains and the writer deleted nothing.
		assertNoDataObject(t, fs, inner)
		assertNoDataDelete(t, fs)
	})

	// TestStreamWriteRecords_ManifestPutError_NoManifest verifies that when Put fails
	// during manifest write, the snapshot remains invisible.
	t.Run("manifest put error no manifest", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		// Create iterator with records
		iter := seqOf(D{"id": "1"}, D{"id": "2"})

		// Inject Put error ONLY for manifest paths (data file Put succeeds)
		fs.SetPutError(testutil.ErrInjectedPut, "manifest")

		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if err == nil {
			t.Fatal("expected error from manifest Put failure")
		}

		// Clear error and verify no snapshot visible
		fs.SetPutError(nil)
		_, err = ds.Latest(t.Context())
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after manifest Put error, got: %v", err)
		}

		// The manifest Put may have stored the manifest, so the data stays.
		assertDataRetained(t, fs)
	})
}

// -----------------------------------------------------------------------------
// Context Cancellation Tests (Deterministic)
// -----------------------------------------------------------------------------
//
// Note: Simple context cancellation without blocking is timing-dependent and
// not deterministically testable with in-memory stores. The tests below use
// blocking to create deterministic cancellation windows.
//
// Context cancellation semantics are timing-sensitive and adapter-dependent
// (docs/maintainers/TEST_MATRIX.md §Streaming Cancellation). The blocked Put test provides
// deterministic coverage; real-world cancellation behavior depends on adapter.

func TestStreamWrite_PutFailure(t *testing.T) {
	// TestStreamWrite_BlockedPut_ContextCancel_NoManifest tests that context
	// cancellation during a blocked Put leaves no manifest.
	t.Run("blocked put context cancel no manifest", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}

		// Create a cancellable context with timeout for safety
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		sw, err := ds.StreamWrite(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Write data
		_, err = sw.Write([]byte("data"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Block CAS (CompareAndSwap) and signal when the pointer CAS is entered.
		// With CAS-aware pointer-before-manifest protocol, the pointer write
		// uses CompareAndSwap, not Put.
		casBlock := make(chan struct{})
		pointerCASEntered := make(chan struct{}, 1)
		fs.SetCASBlock(casBlock)

		// Use beforeCAS hook for deterministic synchronization (no busy-spin)
		fs.SetBeforeCAS(func(path string) {
			if strings.Contains(path, "latest") {
				select {
				case pointerCASEntered <- struct{}{}:
				default:
					// Already signaled
				}
			}
		})

		// Start commit in goroutine (will block on pointer CAS)
		commitDone := make(chan error, 1)
		go func() {
			_, err := sw.Commit(ctx)
			commitDone <- err
		}()

		// Wait for commit to reach the blocked pointer CAS (deterministic sync)
		select {
		case <-pointerCASEntered:
			// Pointer CAS has been called and is now blocked
		case <-time.After(2 * time.Second):
			t.Fatal("commit did not reach pointer CAS")
		}

		// Cancel context while CAS is blocked
		cancel()

		// Unblock CAS (it should see canceled context)
		close(casBlock)

		// Wait for commit to complete
		select {
		case err = <-commitDone:
			if err == nil {
				t.Fatal("expected error from blocked commit with canceled context")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("commit did not complete after unblock")
		}

		// Use fresh context to verify state (parent context was canceled)
		freshCtx, freshCancel := context.WithCancel(t.Context())
		defer freshCancel()

		// Verify: no snapshot visible
		_, err = ds.Latest(freshCtx)
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after blocked+canceled Put, got: %v", err)
		}
	})

	t.Run("data put fails fast returns error", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(testutil.ErrInjectedPut, "/data/")

		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		err = awaitOrFail(t, func() error {
			// Compression may buffer the write; the pipe error then surfaces on Commit.
			if _, werr := sw.Write(make([]byte, 1<<20)); werr != nil {
				return werr
			}
			_, cerr := sw.Commit(t.Context())
			return cerr
		})
		if !errors.Is(err, testutil.ErrInjectedPut) {
			t.Fatalf("got %v, want testutil.ErrInjectedPut", err)
		}

		fs.SetPutError(nil)
		if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after failed stream, got: %v", err)
		}
	})

	// Once Put has returned an error, Write must report it even when the
	// compressor would happily buffer the bytes without touching the pipe.
	t.Run("buffered compressor write fails once put returned", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		putReturned := make(chan struct{})
		fs.SetAfterPut(func(path string, _ error) {
			if strings.Contains(path, "/data/") {
				close(putReturned)
			}
		})
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds", WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		<-putReturned

		// The Put goroutine publishes its result just after the hook fires; a
		// few small writes bound the wait without relying on compressor flushes.
		var werr error
		for range 200 {
			if _, werr = sw.Write([]byte("x")); werr != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !errors.Is(werr, ErrPathExists) {
			t.Fatalf("Write after Put returned: got %v, want ErrPathExists", werr)
		}
		_ = sw.Abort(t.Context())
		assertNoDataDelete(t, fs)
	})
}

func TestStreamWrite_ErrorPrecedence(t *testing.T) {
	// A compressor whose Close fails after Put has already failed must not mask
	// the Put error at Commit.
	t.Run("commit put failure precedes compressor close error", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		gate := afterPutPublished(fs)
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds", WithCompressor(closeFailCompressor{NewZstdCompressor()}))
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		<-gate
		_, err = sw.Commit(t.Context())
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("Commit: got %v, want ErrPathExists", err)
		}
		// Without a Put failure the close error itself surfaces.
		fs2 := testutil.NewFaultStore(NewMemory())
		ds2, err := Open(Use(fs2)).Blobs(t.Context(), "test-ds", WithCompressor(closeFailCompressor{NewZstdCompressor()}))
		if err != nil {
			t.Fatal(err)
		}
		sw2, err := ds2.StreamWrite(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sw2.Commit(t.Context()); !errors.Is(err, errCloseBoom) {
			t.Fatalf("Commit without Put failure: got %v, want errCloseBoom", err)
		}
	})

	// A StreamWriter whose Write failed stays failed: the error is returned again
	// without another attempt on the compressor, so a caller that keeps writing
	// cannot interleave bytes after a gap.
	t.Run("write error is sticky", func(t *testing.T) {
		ctx := t.Context()
		boom := errors.New("compressor refused")
		comp := &writeFailCompressor{Compressor: NewNoOpCompressor(), err: boom}
		ds, err := Open(Use(NewMemory())).Blobs(t.Context(), "sticky", WithCompressor(comp))
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sw.Close() }()

		if _, err := sw.Write([]byte("a")); !errors.Is(err, boom) {
			t.Fatalf("first Write: got %v, want the compressor error", err)
		}
		if _, err := sw.Write([]byte("b")); !errors.Is(err, boom) {
			t.Fatalf("second Write: got %v, want the same error", err)
		}
		if comp.writes != 1 {
			t.Errorf("compressor saw %d writes, want 1 (second Write must not retry)", comp.writes)
		}
	})
}

// -----------------------------------------------------------------------------
// Test Helpers
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Fast Put Failure Tests
// -----------------------------------------------------------------------------
//
// A Put that returns before draining the pipe (ErrPathExists, invalid path, an
// injected error) used to leave the writer blocked forever on the pipe. The
// data-path Put below fails before reading a byte; the write must surface that
// error, not hang (CONTRACT_WRITE_API §StreamWrite / §StreamWriteRecords).

// awaitOrFail runs op and fails the test if it has not returned within the
// deadline, so a regression shows up as a failure rather than a suite timeout.
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

		iter := seqOf(D{"id": "1"}, D{"id": "2"})
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), iter, nil)
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

	// A buffering compressor lets WriteRecord succeed after Put has failed; the
	// record loop must notice the failure instead of following an iterator that
	// never ends.
	t.Run("buffered compressor stops once put returned", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		putReturned := make(chan struct{})
		fs.SetAfterPut(func(path string, _ error) {
			if strings.Contains(path, "/data/") {
				close(putReturned)
			}
		})
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
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

	// When Put has already failed and the iterator then ends with its own error,
	// the storage failure is the root cause and is the error reported.
	t.Run("put failure takes precedence over iterator error", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		gate := afterPutPublished(fs)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), oneThenGate(gate, errIterator), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists to take precedence", err)
		}
		assertNoDataDelete(t, fs)
	})

	// A Put that fails while the iterator is producing its error is still
	// the reported error: the Put result is rechecked after every pull.
	t.Run("put failure during iterator err takes precedence", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		putReturned := make(chan struct{})
		fs.SetAfterPut(func(path string, _ error) {
			if strings.Contains(path, "/data/") {
				close(putReturned)
			}
		})
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}
		// The testutil.FaultStore signals from inside Put, just before it returns; the
		// result is published a moment later. Wait for publication explicitly so
		// the test exercises the re-check rather than a lucky schedule.
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

	// A Put that failed before the iterator ended is reported when it ends.
	t.Run("reported when the iterator ends after the failure", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		fs.SetPutError(ErrPathExists, "/data/")
		gate := afterPutPublished(fs)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D](), WithCompressor(NewZstdCompressor()))
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			_, werr := ds.StreamWrite(t.Context(), oneThenGate(gate, nil), nil)
			return werr
		})
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want ErrPathExists", err)
		}
	})
}

// When Put reports ErrPathExists the object belongs to another writer; the
// failed stream must not delete it (data files are immutable).
// assertDataRetained fails if the commit deleted its data object after a
// manifest write failed: the manifest may have been stored, so the snapshot
// may be visible.
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
		for _, finish := range []struct {
			name string
			fn   func(ctx context.Context, sw StreamWriter) error
		}{
			{"commit", func(ctx context.Context, sw StreamWriter) error { _, err := sw.Commit(ctx); return err }},
			{"abort", func(ctx context.Context, sw StreamWriter) error { return sw.Abort(ctx) }},
		} {
			ctx := t.Context()
			fs := testutil.NewFaultStore(NewMemory())
			fs.SetPutError(ErrPathExists, "/data/")
			ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
			if err != nil {
				t.Fatal(err)
			}
			sw, err := ds.StreamWrite(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = awaitOrFail(t, func() error {
				if _, werr := sw.Write(make([]byte, 1<<20)); werr != nil {
					return finish.fn(ctx, sw)
				}
				return finish.fn(ctx, sw)
			})
			assertNoDataDelete(t, fs)
			if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
				t.Fatalf("%s: expected ErrNoSnapshots, got %v", finish.name, err)
			}
		}
	})

	// A store that spools the stream before checking existence (memory, S3)
	// reports ErrPathExists only after reading; the object is still foreign and
	// must survive cleanup.
	t.Run("read before check store preserves foreign object", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)
		ctx := t.Context()
		// Plant a foreign object at the exact data path just before Put runs, so
		// the memory store reads the whole stream and then refuses to overwrite.
		fs.SetBeforePut(func(path string) {
			if strings.Contains(path, "/data/") {
				_ = inner.Put(ctx, path, strings.NewReader("foreign"))
			}
		})
		ds, err := Open(Use(fs)).Blobs(t.Context(), "test-ds")
		if err != nil {
			t.Fatal(err)
		}
		sw, err := ds.StreamWrite(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = awaitOrFail(t, func() error {
			if _, werr := sw.Write([]byte("ours")); werr != nil {
				return werr
			}
			_, cerr := sw.Commit(ctx)
			return cerr
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

	// An ErrPathExists that the stream itself produced (here, the iterator) is
	// wrapped by the store; the writer cannot tell it from the store's own
	// verdict and does not try to. The store's all-or-nothing Put leaves no
	// object, and the writer deletes nothing either way.
	t.Run("propagated err path exists leaves nothing", func(t *testing.T) {
		inner := NewMemory()
		fs := testutil.NewFaultStore(inner)
		ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		iter := failAfterSeq([]D{{"id": "1"}, {"id": "2"}}, 1, ErrPathExists)
		_, err = ds.StreamWrite(t.Context(), iter, nil)
		if !errors.Is(err, ErrPathExists) {
			t.Fatalf("got %v, want the propagated ErrPathExists", err)
		}
		assertNoDataObject(t, fs, inner)
		assertNoDataDelete(t, fs)
	})
}

// errIterator is the error the test iterators end with.
var errIterator = errors.New("iterator failure")

// failAfterSeq yields the first failAfter records, then err.
func failAfterSeq(records []D, failAfter int, err error) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		for _, r := range records[:failAfter] {
			if !yield(r, nil) {
				return
			}
		}
		yield(nil, err)
	}
}

// endlessAfter yields records forever once the gate is closed; before that
// it yields nothing, so the writer cannot race ahead of the Put failure.
func endlessAfter(gate <-chan struct{}) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		for n := 1; ; n++ {
			<-gate
			if !yield(D{"id": n}, nil) {
				return
			}
		}
	}
}

// oneThenGate yields one record, waits for the gate, and then ends with err
// (nil ends without an error), so that the Put fails while the iterator is
// producing its last result.
func oneThenGate(gate <-chan struct{}, err error) iter.Seq2[D, error] {
	return func(yield func(D, error) bool) {
		if !yield(D{"id": 1}, nil) {
			return
		}
		<-gate
		if err != nil {
			yield(nil, err)
		}
	}
}

// afterPutPublished returns a channel closed shortly after the data-path Put
// has returned, so the test observes the published result, not the hook.
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

// closeFailCompressor wraps a compressor so that Close always fails.
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

// writeFailCompressor hands out a writer whose first Write fails and that
// counts how many Writes reached it.
type writeFailCompressor struct {
	Compressor
	writes int
	err    error
}

func (c *writeFailCompressor) Compress(io.Writer) (io.WriteCloser, error) { return c, nil }
func (c *writeFailCompressor) Write([]byte) (int, error)                  { c.writes++; return 0, c.err }
func (c *writeFailCompressor) Close() error                               { return nil }

// lostManifestResponse stores every manifest and then reports a failure, as
// a store does when the response to a successful Put is lost.
type lostManifestResponse struct{ Store }

func (s lostManifestResponse) Put(ctx context.Context, path string, r io.Reader) error {
	if err := s.Store.Put(ctx, path, r); err != nil || !strings.HasSuffix(path, "manifest.json") {
		return err
	}
	return errors.New("injected: response lost after the manifest was stored")
}

// CompareAndSwap forwards to the memory store so commits use CAS.
func (s lostManifestResponse) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	return s.Store.(ConditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}
