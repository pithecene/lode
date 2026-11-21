package lode_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pithecene/lode/lode"
)

type ctxStrictCleanupStore struct {
	lode.RangeStore
	cancelOnCAS context.CancelFunc
	deleteOK    atomic.Bool
}

func (s *ctxStrictCleanupStore) CompareAndSwap(_ context.Context, _, _, _ string) error {
	if s.cancelOnCAS != nil {
		s.cancelOnCAS()
	}
	return context.Canceled
}

func (s *ctxStrictCleanupStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deleteOK.Store(true)
	return s.RangeStore.Delete(ctx, key)
}

func TestStreamWrite_CanceledCommitDeletesStagedObject(t *testing.T) {
	t.Parallel()

	t.Run("records", func(t *testing.T) {
		mem := lode.NewMemory()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		strict := &ctxStrictCleanupStore{RangeStore: mem, cancelOnCAS: cancel}
		ds, err := lode.Open(lode.Use(strict)).Records(t.Context(), "events", lode.JSONL[lode.D]())
		if err != nil {
			t.Fatal(err)
		}

		seq := func(yield func(lode.D, error) bool) {
			yield(lode.D{"k": "v"}, nil)
		}
		_, err = ds.StreamWrite(ctx, seq, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StreamWrite = %v, want context.Canceled", err)
		}
		if !strict.deleteOK.Load() {
			t.Fatal("expected Delete to receive a non-canceled cleanup context")
		}
		keys, err := mem.List(t.Context(), "datasets/events/snapshots/")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 0 {
			t.Fatalf("staged data object leaked after canceled StreamWrite: %v", keys)
		}
	})

	t.Run("blobs", func(t *testing.T) {
		mem := lode.NewMemory()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		strict := &ctxStrictCleanupStore{RangeStore: mem, cancelOnCAS: cancel}
		blobs, err := lode.Open(lode.Use(strict)).Blobs(t.Context(), "files")
		if err != nil {
			t.Fatal(err)
		}
		_, err = blobs.StreamWrite(ctx, bytes.NewReader([]byte("payload")), nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Blobs.StreamWrite = %v, want context.Canceled", err)
		}
		if !strict.deleteOK.Load() {
			t.Fatal("expected Delete to receive a non-canceled cleanup context")
		}
		keys, err := mem.List(t.Context(), "datasets/files/snapshots/")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 0 {
			t.Fatalf("staged blob leaked after canceled StreamWrite: %v", keys)
		}
	})
}

func TestFS_CanceledContextStopsListing(t *testing.T) {
	t.Parallel()
	store, err := lode.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), "datasets/events/latest", bytes.NewReader([]byte("snap-1"))); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := store.List(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("FS.List with canceled ctx = %v, want context.Canceled", err)
	}
	lister := store.(lode.PrefixLister)
	if _, err := lister.ListPrefixes(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("FS.ListPrefixes with canceled ctx = %v, want context.Canceled", err)
	}
}
