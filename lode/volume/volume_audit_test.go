package volume_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	"github.com/go-openapi/testify/v2/assert"
)

type gatedRangeStore struct {
	lode.RangeStore
	mu              sync.Mutex
	onPut           func(ctx context.Context, key string, r io.Reader) error
	onCAS           func(ctx context.Context, key, expected, replacement string) error
	beforeReadRange func()
	fenceReadErr    error
}

func (g *gatedRangeStore) setOnPut(fn func(ctx context.Context, key string, r io.Reader) error) {
	g.mu.Lock()
	g.onPut = fn
	g.mu.Unlock()
}

func (g *gatedRangeStore) setOnCAS(fn func(ctx context.Context, key, expected, replacement string) error) {
	g.mu.Lock()
	hook := fn
	g.onCAS = hook
	g.mu.Unlock()
}

func (g *gatedRangeStore) Put(ctx context.Context, key string, r io.Reader) error {
	g.mu.Lock()
	hook := g.onPut
	g.mu.Unlock()
	if hook != nil {
		return hook(ctx, key, r)
	}
	return g.RangeStore.Put(ctx, key, r)
}

func (g *gatedRangeStore) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if g.beforeReadRange != nil {
		g.beforeReadRange()
	}
	return g.RangeStore.ReadRange(ctx, key, offset, length)
}

func (g *gatedRangeStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if g.fenceReadErr != nil && strings.HasSuffix(key, "/latest") {
		return nil, g.fenceReadErr
	}
	return g.RangeStore.Get(ctx, key)
}

func (g *gatedRangeStore) CompareAndSwap(ctx context.Context, key, expected, replacement string) error {
	g.mu.Lock()
	hook := g.onCAS
	g.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key, expected, replacement); err != nil {
			return err
		}
	}
	return g.RangeStore.CompareAndSwap(ctx, key, expected, replacement)
}

func openTestVol(t *testing.T, id volume.ID, factory lode.Factory[lode.RangeStore], opts ...volume.Option) volume.Volume {
	t.Helper()
	vol, err := volume.New(id, factory, 1024, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return vol
}

func mustStageBlock(t *testing.T, vol volume.Volume, offset int64, payload string) volume.BlockRef {
	t.Helper()
	blk, err := vol.StageWriteAt(t.Context(), offset, bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}
	return blk
}

func assertReopenCommitErr(
	t *testing.T,
	id volume.ID,
	factory lode.Factory[lode.RangeStore],
	offset int64,
	wantErr error,
	opts ...volume.Option,
) {
	t.Helper()
	vol := openTestVol(t, id, factory, opts...)
	blk := mustStageBlock(t, vol, offset, "xxxx")
	if _, err := vol.Commit(t.Context(), []volume.BlockRef{blk}, nil); !errors.Is(err, wantErr) {
		t.Fatalf("Commit(%s) = %v, want %v", id, err, wantErr)
	}
}

func TestAudit_VolumeCommit_PostPublicationFence(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mem := lode.NewMemory()
	gs := &gatedRangeStore{RangeStore: mem}
	factory := lode.Use[lode.RangeStore](gs)

	vol := openTestVol(t, "vol-1", factory)
	blk := mustStageBlock(t, vol, 0, "hello")

	putErr := errors.New("volume manifest Put response lost")
	entered := make(chan string)
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	gs.setOnPut(func(ctx context.Context, key string, r io.Reader) error {
		if !strings.HasPrefix(key, "volumes/vol-1/snapshots/") || !strings.HasSuffix(key, "/manifest.json") {
			return mem.Put(ctx, key, r)
		}
		entered <- key
		<-release
		if err := mem.Put(ctx, key, r); err != nil {
			return err
		}
		return putErr
	})

	assert.NoGoRoutineLeak(t, func() {
		commitErrCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, cerr := vol.Commit(ctx, []volume.BlockRef{blk}, nil)
			commitErrCh <- cerr
		})

		manifest := <-entered
		id := volume.SnapshotID(strings.TrimSuffix(strings.TrimPrefix(manifest, "volumes/vol-1/snapshots/"), "/manifest.json"))
		if err := volume.Remove(ctx, "vol-1", factory); !errors.Is(err, lode.ErrHeadUncommitted) {
			t.Fatalf("first volume.Remove = %v, want ErrHeadUncommitted", err)
		}
		if err := volume.Remove(ctx, "vol-1", factory); err != nil {
			t.Fatalf("second volume.Remove = %v, want nil", err)
		}
		if exists, err := mem.Exists(ctx, blk.Path); exists || err != nil {
			t.Fatalf("Exists(staged block) after removal = %v, %v, want false, nil", exists, err)
		}
		resume()
		wg.Wait()
		err := <-commitErrCh
		for _, want := range []error{volume.ErrRemoved, history.ErrManifestUncertain, putErr} {
			if !errors.Is(err, want) {
				t.Errorf("Commit after removal = %v, want it to match %v", err, want)
			}
		}
		if _, err := vol.Snapshot(ctx, id); !errors.Is(err, volume.ErrRemoved) && !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("Snapshot(%s) after removal = %v, want ErrRemoved or ErrNotFound", id, err)
		}
		if keys, err := mem.List(ctx, "volumes/vol-1/"); err != nil || !slices.Equal(keys, []string{"volumes/vol-1/latest"}) {
			t.Fatalf("remaining keys = %v (err=%v), want only volumes/vol-1/latest", keys, err)
		}
	})
}

func TestAudit_VolumePendingCompletion_PostPublicationFence(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mem := lode.NewMemory()
	gs := &gatedRangeStore{RangeStore: mem}
	factory := lode.Use[lode.RangeStore](gs)
	vol := openTestVol(t, "vol-pending", factory)
	blk := mustStageBlock(t, vol, 0, "hello")
	putErr := errors.New("volume manifest Put failed")
	var canonical string
	gs.setOnPut(func(ctx context.Context, key string, r io.Reader) error {
		if strings.HasPrefix(key, "volumes/vol-pending/snapshots/") && strings.HasSuffix(key, "/manifest.json") {
			canonical = key
			return putErr
		}
		return mem.Put(ctx, key, r)
	})
	if _, err := vol.Commit(ctx, []volume.BlockRef{blk}, nil); !errors.Is(err, history.ErrManifestUncertain) || !errors.Is(err, putErr) {
		t.Fatalf("initial Commit = %v, want a failed canonical Put", err)
	}

	lostResponse := errors.New("pending completion Put response lost")
	gs.setOnPut(func(ctx context.Context, key string, r io.Reader) error {
		if key != canonical {
			t.Fatalf("pending completion Put path = %q, want %q", key, canonical)
		}
		// Finish removal's deletion pass before the pending completion lands.
		if err := volume.Remove(ctx, vol.ID(), factory); !errors.Is(err, lode.ErrHeadUncommitted) {
			t.Fatalf("first Remove = %v, want ErrHeadUncommitted", err)
		}
		if err := volume.Remove(ctx, vol.ID(), factory); err != nil {
			t.Fatalf("second Remove = %v, want nil", err)
		}
		if err := mem.Put(ctx, key, r); err != nil {
			t.Fatal(err)
		}
		return lostResponse
	})
	_, err := vol.Commit(ctx, []volume.BlockRef{blk}, nil)
	for _, want := range []error{volume.ErrRemoved, history.ErrManifestUncertain, lostResponse} {
		if !errors.Is(err, want) {
			t.Errorf("pending completion Commit = %v, want it to match %v", err, want)
		}
	}
	if keys, err := mem.List(ctx, "volumes/vol-pending/"); err != nil || !slices.Equal(keys, []string{"volumes/vol-pending/latest"}) {
		t.Fatalf("remaining keys = %v (err=%v), want only volumes/vol-pending/latest", keys, err)
	}
}

func TestAudit_VolumeReadAt_MissingBlock(t *testing.T) {
	t.Parallel()
	fenceErr := errors.New("volume fence read failed")
	for _, tc := range []struct {
		name   string
		remove bool
		probe  error
		want   error
	}{
		{name: "removed volume", remove: true, want: volume.ErrRemoved},
		{name: "live volume", want: lode.ErrNotFound},
		{name: "fence read failed", probe: fenceErr, want: lode.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testMissingVolumeBlock(t, tc.remove, tc.probe, tc.want)
		})
	}
}

func testMissingVolumeBlock(t *testing.T, remove bool, probeErr, want error) {
	t.Helper()
	ctx := t.Context()
	mem := lode.NewMemory()
	gs := &gatedRangeStore{RangeStore: mem}
	factory := lode.Use[lode.RangeStore](gs)
	vol := openTestVol(t, "vol-read-race", factory)
	blk := mustStageBlock(t, vol, 0, "hello")
	snap, err := vol.Commit(ctx, []volume.BlockRef{blk}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := vol.ReaderAt(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Delete after ReadAt has loaded the manifest but before it reads the block.
	gs.beforeReadRange = sync.OnceFunc(func() {
		if remove {
			if err := volume.Remove(ctx, vol.ID(), factory); err != nil {
				t.Fatal(err)
			}
		} else if err := mem.Delete(ctx, blk.Path); err != nil {
			t.Fatal(err)
		}
		gs.fenceReadErr = probeErr
	})
	_, readErr := vol.ReadAt(ctx, snap.ID, 0, blk.Length)
	_, readerErr := reader.ReadAt(make([]byte, int(blk.Length)), 0)
	for _, result := range []struct {
		method string
		err    error
	}{
		{method: "Volume.ReadAt", err: readErr},
		{method: "SnapshotReaderAt.ReadAt", err: readerErr},
	} {
		if !errors.Is(result.err, want) || (probeErr != nil && !errors.Is(result.err, probeErr)) {
			t.Errorf("%s = %v, want missing-block outcome %v and fence read error %v", result.method, result.err, want, probeErr)
		}
	}
}

func TestAudit_VolumeChecksumMismatchAndNilReader(t *testing.T) {
	t.Parallel()

	t.Run("StageWriteAt rejects nil and typed-nil readers", func(t *testing.T) {
		mem := lode.NewMemory()
		vol := openTestVol(t, "vol-nil", func() (lode.RangeStore, error) { return mem, nil })
		if _, err := vol.StageWriteAt(t.Context(), 0, nil); !errors.Is(err, lode.ErrInvalidArgument) {
			t.Fatalf("StageWriteAt(nil) = %v, want ErrInvalidArgument", err)
		}
		var typedNil *bytes.Reader
		if _, err := vol.StageWriteAt(t.Context(), 0, typedNil); !errors.Is(err, lode.ErrInvalidArgument) {
			t.Fatalf("StageWriteAt(typedNil) = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("reopen with different or omitted checksum algorithm fails with ErrSettingsMismatch", func(t *testing.T) {
		ctx := t.Context()
		mem := lode.NewMemory()
		factory := func() (lode.RangeStore, error) { return mem, nil }

		vMD5 := openTestVol(t, "vol-cs", factory, volume.WithChecksum(lode.MD5()))
		snap1, err := vMD5.Commit(ctx, []volume.BlockRef{mustStageBlock(t, vMD5, 0, "aaaa")}, nil)
		if err != nil || snap1.Manifest.ChecksumAlgorithm != "md5" {
			t.Fatalf("snap1 = %+v, %v; want md5", snap1, err)
		}

		assertReopenCommitErr(t, "vol-cs", factory, 4, lode.ErrSettingsMismatch, volume.WithChecksum(lode.SHA256()))
		assertReopenCommitErr(t, "vol-cs", factory, 8, lode.ErrSettingsMismatch)

		memPlain := lode.NewMemory()
		plainFactory := func() (lode.RangeStore, error) { return memPlain, nil }
		vPlain := openTestVol(t, "vol-plain", plainFactory)
		if _, err := vPlain.Commit(ctx, []volume.BlockRef{mustStageBlock(t, vPlain, 0, "1111")}, nil); err != nil {
			t.Fatal(err)
		}
		assertReopenCommitErr(t, "vol-plain", plainFactory, 4, lode.ErrSettingsMismatch, volume.WithChecksum(lode.SHA256()))
	})

	t.Run("concurrent first commit refresh rejects mismatched parent checksum", func(t *testing.T) {
		ctx := t.Context()
		mem := lode.NewMemory()
		gs := &gatedRangeStore{RangeStore: mem}
		factory := func() (lode.RangeStore, error) { return gs, nil }

		v1 := openTestVol(t, "vol-race", factory, volume.WithChecksum(lode.MD5()))
		v2 := openTestVol(t, "vol-race", factory, volume.WithChecksum(lode.SHA256()), volume.WithRetry(2))

		b1 := mustStageBlock(t, v1, 0, "aaaa")
		b2 := mustStageBlock(t, v2, 4, "bbbb")

		var intercepted sync.Once
		gs.setOnCAS(func(_ context.Context, key, expected, _ string) error {
			if key == "volumes/vol-race/latest" && expected == "" {
				var trigger bool
				intercepted.Do(func() { trigger = true })
				if trigger {
					gs.setOnCAS(nil)
					if _, cerr := v1.Commit(ctx, []volume.BlockRef{b1}, nil); cerr != nil {
						t.Errorf("v1 Commit = %v", cerr)
					}
				}
			}
			return nil
		})

		if _, err := v2.Commit(ctx, []volume.BlockRef{b2}, nil); !errors.Is(err, lode.ErrSettingsMismatch) {
			t.Fatalf("v2 Commit after retry refresh = %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestAudit_VolumeWithRetryMaxInt(t *testing.T) {
	t.Parallel()
	mem := lode.NewMemory()
	vol := openTestVol(t, "vol-maxretry", func() (lode.RangeStore, error) { return mem, nil }, volume.WithRetry(math.MaxInt))
	snap, err := vol.Commit(t.Context(), []volume.BlockRef{mustStageBlock(t, vol, 0, "ok")}, nil)
	if err != nil || snap == nil || snap.Manifest == nil {
		t.Fatalf("Volume.Commit(WithRetry(MaxInt)) = %v, %v; want committed snapshot", snap, err)
	}
}
