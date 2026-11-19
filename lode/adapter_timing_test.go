package lode

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// FS Adapter Timing Integration Tests
// -----------------------------------------------------------------------------
//
// These tests verify adapter behavior under realistic timing conditions.
// They complement the deterministic fault-injection tests by exercising
// real cancellation and cleanup paths.
//
// Key principle: assert on core invariants (no manifest = no visible snapshot),
// but don't make timing-dependent assertions that could flake.
//
// Contract reference: docs/contracts/CONTRACT_WRITE_API.md

// cancelAfterRead yields data on the first Read, then cancels ctx and returns
// context.Canceled on the subsequent Read.
type cancelAfterRead struct {
	data   []byte
	cancel context.CancelFunc
	sent   bool
}

func (r *cancelAfterRead) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, r.data), nil
	}
	r.cancel()
	return 0, context.Canceled
}

func TestFSAdapter_ContextCancel(t *testing.T) {
	// TestFSAdapter_StreamWrite_ContextCancel_NoSnapshot verifies that when context
	// is canceled during a streaming write, no snapshot becomes visible.
	//
	// This is a timing-sensitive test that exercises real FS behavior rather than
	// fault injection. The core invariant (no manifest = invisible) is deterministic,
	// but cleanup completion timing is adapter-dependent.
	t.Run("stream write context cancel no snapshot", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-timing-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		storeFactory := func() (Store, error) {
			return NewFS(tmpDir)
		}

		ds, err := Open(storeFactory).Blobs(t.Context(), "cancel-test")
		if err != nil {
			t.Fatal(err)
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		r := &cancelAfterRead{data: []byte("data that will be canceled"), cancel: cancel}

		_, err = ds.StreamWrite(cancelCtx, r, D{"test": "cancel"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}

		// Core invariant: no manifest was written so no snapshot is visible.
		freshCtx := t.Context()
		_, err = ds.Latest(freshCtx)
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("expected ErrNoSnapshots after canceled stream write, got: %v", err)
		}
	})

	// TestFSAdapter_StreamWriteRecords_ContextCancel_NoSnapshot verifies that when
	// context is canceled during StreamWriteRecords, no snapshot becomes visible.
	t.Run("stream write records context cancel no snapshot", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-timing-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		storeFactory := func() (Store, error) {
			return NewFS(tmpDir)
		}

		ds, err := Open(storeFactory).Records(t.Context(), "cancel-test", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}

		records := []D{
			{"id": "1", "data": "first"},
			{"id": "2", "data": "second"},
			{"id": "3", "data": "third"},
		}

		cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()

		iter := seqOf(records...)
		_, err = ds.StreamWrite(cancelCtx, iter, D{"test": "cancel"})

		if err == nil {
			freshCtx := t.Context()
			snap, err := ds.Latest(freshCtx)
			if err != nil {
				t.Fatalf("Expected snapshot after successful write, got: %v", err)
			}
			if snap.RowCount != int64(len(records)) {
				t.Errorf("Expected %d rows, got %d", len(records), snap.RowCount)
			}
		} else {
			freshCtx := t.Context()
			_, err := ds.Latest(freshCtx)
			if !errors.Is(err, ErrNoSnapshots) {
				t.Errorf("Expected ErrNoSnapshots after failed write, got: %v", err)
			}
		}
	})

	// TestFSAdapter_Write_ContextCancel_Cleanup verifies that cleanup is attempted
	// when context is canceled during compressed streaming write operations.
	t.Run("write context cancel cleanup", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-timing-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		storeFactory := func() (Store, error) {
			return NewFS(tmpDir)
		}

		ds, err := Open(storeFactory).Blobs(t.Context(), "cleanup-test", WithCompressor(Gzip()))
		if err != nil {
			t.Fatal(err)
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		r := &cancelAfterRead{data: []byte("data"), cancel: cancel}

		_, err = ds.StreamWrite(cancelCtx, r, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}

		time.Sleep(10 * time.Millisecond)

		freshCtx := t.Context()
		_, err = ds.Latest(freshCtx)
		if !errors.Is(err, ErrNoSnapshots) {
			t.Errorf("Expected ErrNoSnapshots after canceled stream write, got: %v", err)
		}
	})
}
