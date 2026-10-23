package lode

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// Tests for commit-path behavior that mutation testing showed unobserved:
// retry option bounds, manifest file ordering, the cold-start latest-pointer
// fast path, and data-file cleanup when the pointer or manifest write fails.
//
// Known equivalent mutant left alive: `string(id) > string(latestID)` → `>=`
// in the scan fallback; a snapshot ID appears once per manifest path, so the
// tie only selects a different manifest of the same snapshot.

func TestRetryOptions_ExactBounds(t *testing.T) {
	cases := []struct {
		name    string
		opt     DatasetOption
		wantErr bool
	}{
		{"base delay zero", WithRetryBaseDelay(0), true},
		{"base delay 1ns", WithRetryBaseDelay(time.Nanosecond), false},
		{"max delay zero", WithRetryMaxDelay(0), true},
		{"max delay 1ns", WithRetryMaxDelay(time.Nanosecond), false},
		{"jitter 0", WithRetryJitter(0), false},
		{"jitter 1", WithRetryJitter(1), false},
		{"jitter just above 1", WithRetryJitter(1.0001), true},
		{"jitter just below 0", WithRetryJitter(-0.0001), true},
	}
	for _, c := range cases {
		_, err := Open(Memory()).Blobs(t.Context(), "test-ds", c.opt)
		if c.wantErr && err == nil {
			t.Errorf("%s: expected error", c.name)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
	}
}

func TestRecords_Write_Commit(t *testing.T) {
	// Manifest.Files is sorted by Path regardless of partition write order.
	t.Run("manifest files sorted by path", func(t *testing.T) {
		ds, err := Open(Memory()).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}
		snap, err := ds.Write(t.Context(), R(D{"id": 1, "day": "c"}, D{"id": 2, "day": "a"}, D{"id": 3, "day": "b"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		paths := make([]string, 0, len(snap.Files))
		for _, f := range snap.Files {
			paths = append(paths, f.Path)
		}
		if len(paths) != 3 || !sort.StringsAreSorted(paths) {
			t.Fatalf("manifest files not sorted by path: %v", paths)
		}
	})

	// A fresh Dataset instance resolves its parent from the persisted pointer
	// without scanning when the pointer names an existing manifest.
	t.Run("cold start valid pointer skips scan", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		first, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := first.Write(t.Context(), R(D{"id": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}

		second, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		before := len(fs.ListCalls())
		snap2, err := second.Write(t.Context(), R(D{"id": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(fs.ListCalls()) - before; got != 0 {
			t.Fatalf("cold start with valid pointer issued %d List calls, want 0", got)
		}
		if snap2.ParentSnapshotID != snap1.ID {
			t.Fatalf("parent = %s, want %s", snap2.ParentSnapshotID, snap1.ID)
		}
	})

	// A failed pointer or manifest write surfaces the store error and leaves no
	// snapshot visible. Data-file cleanup is best-effort and not asserted
	// (PUBLIC_API.md §Cleanup Behavior).
	t.Run("commit failure no snapshot", func(t *testing.T) {
		injectedCAS := errors.New("injected: cas failure")
		cases := []struct {
			name   string
			inject func(fs *testutil.FaultStore)
			clear  func(fs *testutil.FaultStore)
			want   error
		}{
			{"pointer write fails", func(fs *testutil.FaultStore) { fs.SetCASError(injectedCAS) }, func(fs *testutil.FaultStore) { fs.SetCASError(nil) }, injectedCAS},
			{"manifest write fails", func(fs *testutil.FaultStore) { fs.SetPutError(testutil.ErrInjectedPut, "manifest.json") }, func(fs *testutil.FaultStore) { fs.SetPutError(nil) }, testutil.ErrInjectedPut},
		}
		for _, c := range cases {
			fs := testutil.NewFaultStore(NewMemory())
			ds, err := Open(Use(fs)).Records(t.Context(), "test-ds", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}
			c.inject(fs)
			if _, err := ds.Write(t.Context(), R(D{"id": 1}), nil); !errors.Is(err, c.want) {
				t.Fatalf("%s: got %v, want %v", c.name, err, c.want)
			}
			c.clear(fs)
			if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
				t.Fatalf("%s: expected ErrNoSnapshots, got %v", c.name, err)
			}
		}
	})
}
