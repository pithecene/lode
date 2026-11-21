package volume

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"
)

// Every caller-supplied value a Volume refuses is reported as
// lode.ErrInvalidArgument (CONTRACT_ERRORS.md §Configuration Errors), worded
// as lode's own argument errors are: one "lode: " prefix and the value named.
func TestErrInvalidArgument_Classification(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	factory := memoryFactoryFrom(lode.NewMemory())
	nilStore := func() (lode.RangeStore, error) { return nil, nil } //nolint:nilnil // the misbehaving factory under test

	vol, err := New("vol", factory, 16)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("0123")))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := vol.Commit(ctx, []BlockRef{staged}, nil)
	if err != nil {
		t.Fatal(err)
	}

	commitBlock := func(block BlockRef) func() error {
		return func() error {
			_, err := vol.Commit(ctx, []BlockRef{block}, nil)
			return err
		}
	}
	cases := []struct {
		name string
		call func() error
		want string // substring naming the value
	}{
		{"New nil factory", func() error { _, err := New("v", nil, 1); return err }, "store factory"},
		{"New empty ID", func() error { _, err := New("", factory, 1); return err }, "volume ID"},
		{"New ID escaping the volumes directory", func() error { _, err := New("../datasets/events", factory, 1); return err }, "volume ID"},
		{"New ID with a slash", func() error { _, err := New("a/data", factory, 1); return err }, "volume ID"},
		{"New dot ID", func() error { _, err := New("..", factory, 1); return err }, "volume ID"},
		{"New ID with a backslash", func() error { _, err := New(`a\b`, factory, 1); return err }, "volume ID"},
		{"New ID with the reserved prefix", func() error { _, err := New(".lode-tmp-x", factory, 1); return err }, "volume ID"},
		{"New non-positive length", func() error { _, err := New("v", factory, 0); return err }, "total length"},
		{"New nil store", func() error { _, err := New("v", nilStore, 1); return err }, "nil store"},
		{"WithRetry negative", func() error { _, err := New("v", factory, 1, WithRetry(-1)); return err }, "WithRetry: n must be non-negative"},
		{"WithRetryBaseDelay zero", func() error { _, err := New("v", factory, 1, WithRetryBaseDelay(0)); return err }, "WithRetryBaseDelay: delay must be positive"},
		{"WithRetryMaxDelay zero", func() error { _, err := New("v", factory, 1, WithRetryMaxDelay(0)); return err }, "WithRetryMaxDelay: delay must be positive"},
		{"WithRetryJitter out of range", func() error { _, err := New("v", factory, 1, WithRetryJitter(2)); return err }, "WithRetryJitter: jitter must be between 0.0 and 1.0"},
		{"WithRetryObserver nil", func() error { _, err := New("v", factory, 1, WithRetryObserver(nil)); return err }, "WithRetryObserver: fn must not be nil"},
		{"StageWriteAt negative offset", func() error { _, err := vol.StageWriteAt(ctx, -1, bytes.NewReader([]byte("x"))); return err }, "offset"},
		{"StageWriteAt empty block", func() error { _, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(nil)); return err }, "must not be empty"},
		{"StageWriteAt beyond volume", func() error { _, err := vol.StageWriteAt(ctx, 15, bytes.NewReader([]byte("xy"))); return err }, "address space"},
		{"Commit int metadata", func() error { _, err := vol.Commit(ctx, []BlockRef{staged}, 123); return err }, "metadata"},
		{"Commit slice metadata", func() error { _, err := vol.Commit(ctx, []BlockRef{staged}, []int{1}); return err }, "metadata"},
		{"Commit unmarshalable metadata", func() error { _, err := vol.Commit(ctx, []BlockRef{staged}, func() {}); return err }, "metadata"},
		{"Commit no blocks", func() error { _, err := vol.Commit(ctx, nil, nil); return err }, "at least one new block"},
		{"Commit already committed", func() error { _, err := vol.Commit(ctx, []BlockRef{staged}, nil); return err }, "already committed"},
		{"Commit negative offset", commitBlock(BlockRef{Offset: -1, Length: 1, Path: "p"}), "offset"},
		{"Commit non-positive length", commitBlock(BlockRef{Offset: 4, Length: 0, Path: "p"}), "length"},
		{"Commit beyond volume", commitBlock(BlockRef{Offset: 4, Length: 100, Path: "p"}), "address space"},
		{"Commit foreign path", commitBlock(BlockRef{Offset: 4, Length: 4, Path: "elsewhere"}), "layout path"},
		{"ReadAt negative offset", func() error { _, err := vol.ReadAt(ctx, snap.ID, -1, 1); return err }, "offset"},
		{"ReadAt non-positive length", func() error { _, err := vol.ReadAt(ctx, snap.ID, 0, 0); return err }, "length"},
		{"ReadAt beyond volume", func() error { _, err := vol.ReadAt(ctx, snap.ID, 0, 17); return err }, "address space"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			if !errors.Is(err, lode.ErrInvalidArgument) {
				t.Fatalf("got %v, want lode.ErrInvalidArgument", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("message %q does not name the value (%q)", err, c.want)
			}
			if !strings.HasPrefix(err.Error(), "lode: ") || strings.Contains(err.Error(), "lode: lode:") {
				t.Errorf("message %q must carry exactly one lode: prefix", err)
			}
		})
	}
}

// Argument errors come before any store work: an invalid option is reported
// even when the factory would fail, and the factory is not called.
func TestErrInvalidArgument_BeforeStoreFactory(t *testing.T) {
	t.Parallel()
	calls := 0
	failing := func() (lode.RangeStore, error) { calls++; return nil, errors.New("factory down") }

	_, err := New("v", failing, 1, WithRetry(-1))
	if !errors.Is(err, lode.ErrInvalidArgument) {
		t.Fatalf("got %v, want lode.ErrInvalidArgument", err)
	}
	if calls != 0 {
		t.Errorf("store factory called %d times before argument validation", calls)
	}
}

// The Volume sentinels keep the messages they had in package lode.
func TestSentinels_Messages(t *testing.T) {
	t.Parallel()
	if !errors.Is(ErrRemoved, lode.ErrRemoved) {
		t.Error("expected volume.ErrRemoved to match lode.ErrRemoved")
	}
	for _, c := range []struct {
		err  error
		want string
	}{
		{ErrRangeMissing, "range missing: requested range is not fully committed"},
		{ErrOverlappingBlocks, "overlapping blocks in cumulative manifest"},
		{ErrRemoved, "removed"},
	} {
		if c.err.Error() != c.want {
			t.Errorf("got %q, want %q", c.err, c.want)
		}
	}
}
