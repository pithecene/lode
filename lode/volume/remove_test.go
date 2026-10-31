package volume

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// pointerContent reads volume id's head pointer as stored.
func pointerContent(t *testing.T, store lode.Store, id ID) string {
	t.Helper()
	rc, err := store.Get(t.Context(), volumeLatestPointerPath(id))
	if err != nil {
		t.Fatalf("reading the pointer: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// listed returns every key under prefix, sorted.
func listed(t *testing.T, store lode.Store, prefix string) []string {
	t.Helper()
	keys, err := store.List(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// A removed volume keeps only its pointer, which holds the removal marker.
// Every handle then refuses its head and its commits, and a volume whose ID
// extends the removed one is untouched.
func TestRemove_LeavesOnlyTheFence(t *testing.T) {
	ctx := t.Context()
	store := lode.NewMemory()
	factory := lode.Use(store)
	vol := newTestVolume(t, "a", factory, 20)
	snap := commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))}, Metadata{})
	commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))}, Metadata{})
	sibling := newTestVolume(t, "ab", factory, 5)
	commitBlocks(t, sibling, []BlockRef{stageBlock(t, sibling, 0, []byte("CCCCC"))}, Metadata{})

	if err := Remove(ctx, "a", factory); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if got, want := listed(t, store, "volumes/a/"), []string{"volumes/a/latest"}; !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
	if got := pointerContent(t, store, "a"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the removal marker", got)
	}
	if _, err := vol.Latest(ctx); !errors.Is(err, ErrRemoved) {
		t.Errorf("Latest on the open handle = %v, want ErrRemoved", err)
	}
	reopened := newTestVolume(t, "a", factory, 20)
	if _, err := reopened.Latest(ctx); !errors.Is(err, ErrRemoved) {
		t.Errorf("Latest on a new handle = %v, want ErrRemoved", err)
	}
	blk := stageBlock(t, reopened, 0, []byte("x"))
	if _, err := reopened.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, ErrRemoved) {
		t.Errorf("Commit on a new handle = %v, want ErrRemoved", err)
	}
	if _, err := reopened.Snapshot(ctx, snap.ID); !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("Snapshot of a removed snapshot = %v, want ErrNotFound", err)
	}
	if latest, err := sibling.Latest(ctx); err != nil || len(latest.Manifest.Blocks) != 1 {
		t.Fatalf("sibling volume: %v, %v", latest, err)
	}
}

// Removing a volume that was never written writes the fence, so no later
// commit can start its history.
func TestRemove_FencesAVolumeNeverWritten(t *testing.T) {
	ctx := t.Context()
	store := lode.NewMemory()
	if err := Remove(ctx, "fresh", lode.Use(store)); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	vol := newTestVolume(t, "fresh", lode.Use(store), 4)
	blk := stageBlock(t, vol, 0, []byte("data"))
	if _, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, ErrRemoved) {
		t.Fatalf("Commit after removal = %v, want ErrRemoved", err)
	}
}

// A removal that fails part way keeps the fence, and a second call deletes
// what remains, including a block staged into the volume after the first.
func TestRemove_CompletesAnInterruptedRemoval(t *testing.T) {
	ctx := t.Context()
	fs := testutil.NewFaultStore(lode.NewMemory())
	factory := newFaultStoreFactory(fs)
	vol := newTestVolume(t, "v", factory, 10)
	commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})

	fs.SetDeleteError(testutil.ErrInjectedDelete)
	if err := Remove(ctx, "v", factory); !errors.Is(err, testutil.ErrInjectedDelete) {
		t.Fatalf("Remove with a failing delete = %v, want the injected error", err)
	}
	if got := pointerContent(t, fs, "v"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q after a failed removal, want the marker", got)
	}
	// The fence is up while the manifests remain. A writer that remembers
	// its head commits without reading the pointer, and meets the marker at
	// its swap: it reports the removal, not a conflict, with no retries.
	blk := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 5))
	if _, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, ErrRemoved) {
		t.Fatalf("Commit by the handle that remembers its head = %v, want ErrRemoved", err)
	}

	fs.SetDeleteError(nil)
	if err := Remove(ctx, "v", factory); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if got, want := listed(t, fs, "volumes/v/"), []string{"volumes/v/latest"}; !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
}

// A removal that fences a commit which advanced the pointer but has not
// persisted its manifest refuses to delete: the fence is up, the commit can
// finish, and the next removal deletes what then exists.
func TestRemove_RefusesWhileACommitIsPending(t *testing.T) {
	ctx := t.Context()
	fs := testutil.NewFaultStore(lode.NewMemory())
	factory := newFaultStoreFactory(fs)
	vol := newTestVolume(t, "v", factory, 10)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))

	// The commit advances the pointer and then fails to persist its
	// manifest, which leaves exactly a pending commit's state.
	fs.SetPutError(testutil.ErrInjectedPut, "manifest.json")
	if _, err := vol.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, testutil.ErrInjectedPut) {
		t.Fatalf("the commit: %v, want the injected put error", err)
	}
	fs.SetPutError(nil)

	err := Remove(ctx, "v", factory)
	if !errors.Is(err, lode.ErrHeadUncommitted) {
		t.Fatalf("Remove over a pending commit = %v, want ErrHeadUncommitted", err)
	}
	if got := pointerContent(t, fs, "v"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the marker", got)
	}
	if exists, _ := fs.Exists(ctx, blk.Path); !exists {
		t.Fatal("the refused removal deleted the pending commit's block")
	}

	// The commit finishes: its manifest is persisted after the fence.
	staged, err := fs.Get(ctx, volumeStagedManifestPath("v", SnapshotID(stagedID(t, fs, "v"))))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(staged)
	_ = staged.Close()
	if err := fs.Put(ctx, volumeManifestPath("v", SnapshotID(stagedID(t, fs, "v"))), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if err := Remove(ctx, "v", factory); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if got, want := listed(t, fs, "volumes/v/"), []string{"volumes/v/latest"}; !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
}

// stagedID returns the one staged manifest's snapshot ID of volume id.
func stagedID(t *testing.T, store lode.Store, id ID) string {
	t.Helper()
	keys := listed(t, store, "volumes/"+string(id)+"/staged/")
	if len(keys) != 1 {
		t.Fatalf("staged manifests %q, want one", keys)
	}
	return strings.TrimSuffix(strings.TrimPrefix(keys[0], "volumes/"+string(id)+"/staged/"), ".json")
}

// A store that reports a deleted object as missing, as when two removals
// overlap, does not fail the removal.
func TestRemove_TreatsAMissingObjectAsDeleted(t *testing.T) {
	ctx := t.Context()
	fs := testutil.NewFaultStore(lode.NewMemory())
	factory := newFaultStoreFactory(fs)
	vol := newTestVolume(t, "v", factory, 10)
	commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})
	fs.SetDeleteError(lode.ErrNotFound)
	if err := Remove(ctx, "v", factory); err != nil {
		t.Fatalf("Remove over a store that reports missing objects: %v", err)
	}
	if got := pointerContent(t, fs, "v"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the marker", got)
	}
}

// The largest retry count writes the fence as any other: the attempt loop
// counts up to the attempts and never past them.
func TestRemove_WritesTheFenceAtTheLargestRetryCount(t *testing.T) {
	ctx := t.Context()
	store := lode.NewMemory()
	factory := lode.Use(store)
	vol := newTestVolume(t, "v", factory, 10)
	commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})
	if err := Remove(ctx, "v", factory, WithRetryCount(math.MaxInt)); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := pointerContent(t, store, "v"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the marker", got)
	}
	if got, want := listed(t, store, "volumes/v/"), []string{"volumes/v/latest"}; !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
}

// A writer that held the volume open through the removal can never commit:
// its swap conflicts with the fence, and its retry reads the removal.
func TestRemove_FencesAWriterThatHeldTheVolume(t *testing.T) {
	ctx := t.Context()
	store := lode.NewMemory()
	factory := lode.Use(store)
	stale := newTestVolume(t, "v", factory, 10, WithRetryCount(2))
	commitBlocks(t, stale, []BlockRef{stageBlock(t, stale, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})

	if err := Remove(ctx, "v", factory); err != nil {
		t.Fatal(err)
	}
	blk := stageBlock(t, stale, 5, bytes.Repeat([]byte("B"), 5))
	if _, err := stale.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, ErrRemoved) {
		t.Fatalf("Commit by the stale writer = %v, want ErrRemoved", err)
	}
	if got := pointerContent(t, store, "v"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the marker", got)
	}

	// Without retries too: the writer's remembered parent has no manifest
	// left, so it reads the pointer again and finds the removal.
	noRetry := newTestVolume(t, "w", factory, 10)
	commitBlocks(t, noRetry, []BlockRef{stageBlock(t, noRetry, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})
	if err := Remove(ctx, "w", factory); err != nil {
		t.Fatal(err)
	}
	blk = stageBlock(t, noRetry, 5, bytes.Repeat([]byte("B"), 5))
	if _, err := noRetry.Commit(ctx, []BlockRef{blk}, Metadata{}); !errors.Is(err, ErrRemoved) {
		t.Fatalf("Commit without retries = %v, want ErrRemoved", err)
	}
	if got := pointerContent(t, store, "w"); got != history.RemovedPointer {
		t.Fatalf("pointer holds %q, want the marker", got)
	}
}

// A commit that lands between the removal's read of the pointer and its
// swap makes the swap conflict. Without retries the removal returns the
// conflict and deletes nothing; with retries it reads the new head and
// removes the volume, the new commit included.
func TestRemove_RetriesAConflictingCommit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []Option
		wantErr error
	}{
		{"without retries", nil, lode.ErrSnapshotConflict},
		{"with retries", []Option{WithRetryCount(2), WithRetryBaseDelay(time.Millisecond)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			fs := testutil.NewFaultStore(lode.NewMemory())
			factory := newFaultStoreFactory(fs)
			writer := newTestVolume(t, "v", factory, 10)
			commitBlocks(t, writer, []BlockRef{stageBlock(t, writer, 0, bytes.Repeat([]byte("A"), 5))}, Metadata{})
			blk := stageBlock(t, writer, 5, bytes.Repeat([]byte("B"), 5))

			raced := false
			fs.SetBeforeCAS(func(p string) {
				if raced || p != volumeLatestPointerPath("v") {
					return
				}
				raced = true
				fs.SetBeforeCAS(nil)
				commitBlocks(t, writer, []BlockRef{blk}, Metadata{})
			})
			err := Remove(ctx, "v", factory, tc.opts...)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Remove = %v, want %v", err, tc.wantErr)
				}
				if latest, err := writer.Latest(ctx); err != nil || len(latest.Manifest.Blocks) != 2 {
					t.Fatalf("a refused removal changed the volume: %v, %v", latest, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Remove: %v", err)
			}
			if got, want := listed(t, fs, "volumes/v/"), []string{"volumes/v/latest"}; !slices.Equal(got, want) {
				t.Fatalf("left %q, want %q", got, want)
			}
		})
	}
}

// Arguments are checked before the store is created.
func TestRemove_ChecksItsArguments(t *testing.T) {
	ctx := t.Context()
	created := false
	factory := func() (lode.RangeStore, error) { created = true; return lode.NewMemory(), nil }
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"nil factory", Remove(ctx, "v", nil)},
		{"an ID that is not one segment", Remove(ctx, "../datasets/x", factory)},
		{"an empty ID", Remove(ctx, "", factory)},
		{"a negative retry count", Remove(ctx, "v", factory, WithRetryCount(-1))},
	} {
		if !errors.Is(tc.err, lode.ErrInvalidArgument) {
			t.Errorf("%s: %v, want ErrInvalidArgument", tc.name, tc.err)
		}
	}
	if created {
		t.Fatal("the store was created for invalid arguments")
	}

	boom := errors.New("boom")
	if err := Remove(ctx, "v", func() (lode.RangeStore, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Errorf("a failing factory: %v, want its error", err)
	}
	typedNil := func() (lode.RangeStore, error) { var s *testutil.FaultStore; return s, nil }
	if err := Remove(ctx, "v", typedNil); !errors.Is(err, lode.ErrInvalidArgument) {
		t.Errorf("a typed nil store: %v, want ErrInvalidArgument", err)
	}
}
