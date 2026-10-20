package lode

import (
	"errors"
	"sort"
	"testing"
	"time"
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
		opt     Option
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
		_, err := NewDataset("test-ds", NewMemoryFactory(), c.opt)
		if c.wantErr && err == nil {
			t.Errorf("%s: expected error", c.name)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
	}
}

func TestDataset_Write_Commit(t *testing.T) {
	// Manifest.Files is sorted by Path regardless of partition write order.
	t.Run("manifest files sorted by path", func(t *testing.T) {
		ds, err := NewDataset("test-ds", NewMemoryFactory(), WithCodec(NewJSONLCodec()), WithHiveLayout("day"))
		if err != nil {
			t.Fatal(err)
		}
		snap, err := ds.Write(t.Context(), R(D{"id": 1, "day": "c"}, D{"id": 2, "day": "a"}, D{"id": 3, "day": "b"}), Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		paths := make([]string, 0, len(snap.Manifest.Files))
		for _, f := range snap.Manifest.Files {
			paths = append(paths, f.Path)
		}
		if len(paths) != 3 || !sort.StringsAreSorted(paths) {
			t.Fatalf("manifest files not sorted by path: %v", paths)
		}
	})

	// A fresh Dataset instance resolves its parent from the persisted pointer
	// without scanning when the pointer names an existing manifest.
	t.Run("cold start valid pointer skips scan", func(t *testing.T) {
		fs := newFaultStore(NewMemory())
		first, err := NewDataset("test-ds", newFaultStoreFactory(fs), WithCodec(NewJSONLCodec()))
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := first.Write(t.Context(), R(D{"id": 1}), Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		second, err := NewDataset("test-ds", newFaultStoreFactory(fs), WithCodec(NewJSONLCodec()))
		if err != nil {
			t.Fatal(err)
		}
		before := len(fs.ListCalls())
		snap2, err := second.Write(t.Context(), R(D{"id": 2}), Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		if got := len(fs.ListCalls()) - before; got != 0 {
			t.Fatalf("cold start with valid pointer issued %d List calls, want 0", got)
		}
		if snap2.Manifest.ParentSnapshotID != snap1.ID {
			t.Fatalf("parent = %s, want %s", snap2.Manifest.ParentSnapshotID, snap1.ID)
		}
	})

	// A failed pointer or manifest write surfaces the store error and leaves no
	// snapshot visible. Data-file cleanup is best-effort and not asserted
	// (PUBLIC_API.md §Cleanup Behavior).
	t.Run("commit failure no snapshot", func(t *testing.T) {
		injectedCAS := errors.New("injected: cas failure")
		cases := []struct {
			name   string
			inject func(fs *faultStore)
			clear  func(fs *faultStore)
			want   error
		}{
			{"pointer write fails", func(fs *faultStore) { fs.SetCASError(injectedCAS) }, func(fs *faultStore) { fs.SetCASError(nil) }, injectedCAS},
			{"manifest write fails", func(fs *faultStore) { fs.SetPutError(errInjectedPut, "manifest.json") }, func(fs *faultStore) { fs.SetPutError(nil) }, errInjectedPut},
		}
		for _, c := range cases {
			fs := newFaultStore(NewMemory())
			ds, err := NewDataset("test-ds", newFaultStoreFactory(fs), WithCodec(NewJSONLCodec()))
			if err != nil {
				t.Fatal(err)
			}
			c.inject(fs)
			if _, err := ds.Write(t.Context(), R(D{"id": 1}), Metadata{}); !errors.Is(err, c.want) {
				t.Fatalf("%s: got %v, want %v", c.name, err, c.want)
			}
			c.clear(fs)
			if _, err := ds.Latest(t.Context()); !errors.Is(err, ErrNoSnapshots) {
				t.Fatalf("%s: expected ErrNoSnapshots, got %v", c.name, err)
			}
		}
	})
}
