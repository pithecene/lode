package lode

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

func TestFSCAS_HonorsContextCancellationWhileContended(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			t.Skipf("CompareAndSwap unsupported: %v", err)
		}
		t.Fatal(err)
	}

	// Hold the companion .lock file via an independent open file descriptor.
	lockPath := filepath.Join(tmpDir, "ptr", "latest.lock")
	holder, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer closer(holder)()

	unlock, err := lockExclusive(ctx, holder)
	if err != nil {
		t.Fatal(err)
	}
	unlocked := false
	defer func() {
		if !unlocked {
			unlock()
		}
	}()

	shortCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()

	start := time.Now()
	casErr := store.CompareAndSwap(shortCtx, "ptr/latest", "snap-1", "snap-2")
	elapsed := time.Since(start)

	if !errors.Is(casErr, context.DeadlineExceeded) {
		t.Fatalf("contended CompareAndSwap err = %v, want context.DeadlineExceeded", casErr)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("contended CompareAndSwap took %s, expected prompt cancellation", elapsed)
	}

	// Releasing the lock afterwards must NOT publish the canceled replacement.
	unlock()
	unlocked = true

	if got := readOrAbsent(t, store, "ptr/latest"); got != "snap-1" {
		t.Fatalf("pointer after canceled CAS = %q, want snap-1", got)
	}
}

func TestStore_IntermediateTempNamedDirListedAcrossBackends(t *testing.T) {
	forEachStore(t, func(t *testing.T, store RangeStore) {
		ctx := t.Context()
		if err := store.Put(ctx, ".lode-tmp-dir/payload", bytes.NewReader([]byte("ok"))); err != nil {
			t.Fatalf("Put(.lode-tmp-dir/payload): %v", err)
		}

		keys, err := store.List(ctx, "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !slices.Equal(keys, []string{".lode-tmp-dir/payload"}) {
			t.Fatalf("List(\"\") = %v, want [.lode-tmp-dir/payload]", keys)
		}

		pl := store.(PrefixLister)
		prefixes, err := pl.ListPrefixes(ctx, "")
		if err != nil {
			t.Fatalf("ListPrefixes: %v", err)
		}
		if !slices.Equal(prefixes, []string{".lode-tmp-dir/"}) {
			t.Fatalf("ListPrefixes(\"\") = %v, want [.lode-tmp-dir/]", prefixes)
		}
	})
}

func TestStore_DirectoryPrefixIsNotAnObject(t *testing.T) {
	forEachStore(t, func(t *testing.T, store RangeStore) {
		ctx := t.Context()
		if err := store.Put(ctx, "parent/child", bytes.NewReader([]byte("leaf"))); err != nil {
			t.Fatalf("Put(parent/child): %v", err)
		}

		exists, err := store.Exists(ctx, "parent")
		if err != nil || exists {
			t.Errorf("Exists(parent) = %v, %v; want false, nil", exists, err)
		}
		if _, err := store.Get(ctx, "parent"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(parent) = %v, want ErrNotFound", err)
		}
		if _, err := store.ReadRange(ctx, "parent", 0, 4); !errors.Is(err, ErrNotFound) {
			t.Errorf("ReadRange(parent) = %v, want ErrNotFound", err)
		}
		if _, err := store.ReaderAt(ctx, "parent"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ReaderAt(parent) = %v, want ErrNotFound", err)
		}
		if err := store.Delete(ctx, "parent"); err != nil {
			t.Errorf("Delete(parent) = %v, want nil", err)
		}
		if got := readOrAbsent(t, store, "parent/child"); got != "leaf" {
			t.Errorf("parent/child after Delete(parent) = %q, want leaf", got)
		}
	})
}

func TestFS_RootContainmentBoundary(t *testing.T) {
	rootPath := string(filepath.Separator)
	if vol := filepath.VolumeName(os.TempDir()); vol != "" {
		rootPath = vol + string(filepath.Separator)
	}
	store, err := NewFS(rootPath)
	if err != nil {
		t.Fatalf("NewFS(%q): %v", rootPath, err)
	}
	fs := store.(*fsStore)

	gotFile, err := fs.safePathForFile("a/b.txt")
	if err != nil {
		t.Fatalf("safePathForFile on filesystem root = %v, want nil", err)
	}
	wantFile := filepath.Join(rootPath, "a", "b.txt")
	if gotFile != wantFile {
		t.Errorf("safePathForFile = %q, want %q", gotFile, wantFile)
	}

	if _, err := fs.safePathForPrefix("a/"); err != nil {
		t.Errorf("safePathForPrefix(\"a/\") on filesystem root = %v, want nil", err)
	}
	if _, err := fs.safePathForFile("../escape"); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("safePathForFile(\"../escape\") = %v, want ErrInvalidPath", err)
	}
}

func assertKeyOpsRejectInvalidPath(t *testing.T, store RangeStore, key string) {
	t.Helper()
	ctx := t.Context()
	if err := store.Put(ctx, key, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("Put(%q) = %v, want ErrInvalidPath", key, err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("Get(%q) = %v, want ErrInvalidPath", key, err)
	}
	if _, err := store.Exists(ctx, key); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("Exists(%q) = %v, want ErrInvalidPath", key, err)
	}
	if err := store.Delete(ctx, key); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("Delete(%q) = %v, want ErrInvalidPath", key, err)
	}
	if _, err := store.ReadRange(ctx, key, 0, 1); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("ReadRange(%q) = %v, want ErrInvalidPath", key, err)
	}
	if _, err := store.ReaderAt(ctx, key); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("ReaderAt(%q) = %v, want ErrInvalidPath", key, err)
	}
	if err := store.CompareAndSwap(ctx, key, "", "v"); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("CompareAndSwap(%q) = %v, want ErrInvalidPath", key, err)
	}
}

func TestStore_CanonicalPathRejectionParity(t *testing.T) {
	backends := append(slices.Clone(casBackends), storeBackend{
		name: "single-process non-cas fs",
		open: func(t *testing.T) RangeStore {
			fs := openFSStore(t)
			return SingleProcess(struct {
				ObjectStore
				RangeReader
			}{testutil.WithoutCAS(fs), fs}).(RangeStore)
		},
	})

	invalidKeys := []string{
		"",
		"/",
		"/a",
		"a/",
		".",
		"./a",
		"a/./b",
		"..",
		"../a",
		"a/../b",
		"a//b",
		`a\b`,
		".lode-tmp-1",
		"dir/.lode-tmp-1",
	}
	invalidPrefixes := []string{
		"/",
		"/a",
		".",
		"./",
		"a/./b",
		"..",
		"../a",
		"a/../b",
		"a//b",
		"a//",
		`a\b`,
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			ctx := t.Context()
			store := b.open(t)
			for _, k := range invalidKeys {
				assertKeyOpsRejectInvalidPath(t, store, k)
			}
			for _, p := range invalidPrefixes {
				if _, err := store.List(ctx, p); !errors.Is(err, ErrInvalidPath) {
					t.Errorf("List(%q) = %v, want ErrInvalidPath", p, err)
				}
				if pl, ok := store.(PrefixLister); ok {
					if _, err := pl.ListPrefixes(ctx, p); !errors.Is(err, ErrInvalidPath) {
						t.Errorf("ListPrefixes(%q) = %v, want ErrInvalidPath", p, err)
					}
				}
			}
		})
	}
}
