package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// TestSingleProcess_NilStore checks that a nil store is returned as nil, so
// that a constructor's nil-store check rejects it.
func TestSingleProcess_NilStore(t *testing.T) {
	if got := SingleProcess(nil); got != nil {
		t.Errorf("SingleProcess(nil) = %v, want nil", got)
	}
}

// pausedDelete deletes a path and then waits until release is closed,
// holding a replacement between its Delete and its Put.
type pausedDelete struct {
	testutil.Store
	deleted, release chan struct{}
}

func (p *pausedDelete) Delete(ctx context.Context, path string) error {
	if err := p.Store.Delete(ctx, path); err != nil {
		return err
	}
	close(p.deleted)
	<-p.release
	return nil
}

// replacementReads are the read methods of the returned store, each
// reporting what it observes at ptr/latest.
var replacementReads = []struct {
	name string
	read func(ctx context.Context, s Store) string
}{
	{"Get", func(ctx context.Context, s Store) string {
		rc, err := s.Get(ctx, "ptr/latest")
		if err != nil {
			return err.Error()
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		return string(data)
	}},
	{"Exists", func(ctx context.Context, s Store) string {
		ok, err := s.Exists(ctx, "ptr/latest")
		return fmt.Sprint(ok, err)
	}},
	{"List", func(ctx context.Context, s Store) string {
		paths, err := s.List(ctx, "ptr/")
		return fmt.Sprintf("%s %v", strings.Join(paths, ","), err)
	}},
	{"ReadRange", func(ctx context.Context, s Store) string {
		data, err := s.ReadRange(ctx, "ptr/latest", 0, 6)
		if err != nil {
			return err.Error()
		}
		return string(data)
	}},
	{"ReaderAt", func(ctx context.Context, s Store) string {
		ra, err := s.ReaderAt(ctx, "ptr/latest")
		if err != nil {
			return err.Error()
		}
		buf := make([]byte, 6)
		if _, err := ra.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			return err.Error()
		}
		return string(buf)
	}},
}

// TestSingleProcess_ReadsWaitForReplacement checks that a read through the
// returned store never observes a path as absent between the Delete and the
// Put of a replacement: it waits, and then observes the new content.
func TestSingleProcess_ReadsWaitForReplacement(t *testing.T) {
	want := map[string]string{
		"Get": "snap-2", "Exists": "true <nil>", "List": "ptr/latest <nil>",
		"ReadRange": "snap-2", "ReaderAt": "snap-2",
	}
	for _, r := range replacementReads {
		t.Run(r.name, func(t *testing.T) {
			ctx := t.Context()
			inner := &pausedDelete{
				Store:   testutil.WithoutCAS(NewMemory()),
				deleted: make(chan struct{}), release: make(chan struct{}),
			}
			store := SingleProcess(inner)
			cw := store.(ConditionalWriter)
			if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
				t.Fatal(err)
			}

			replaced := make(chan error, 1)
			go func() { replaced <- cw.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2") }()
			<-inner.deleted
			observed := make(chan string, 1)
			go func() { observed <- r.read(ctx, store) }()

			// A read that returns while the path is deleted observed the gap.
			select {
			case got := <-observed:
				close(inner.release)
				t.Fatalf("%s returned during the replacement and observed %q", r.name, got)
			case <-time.After(50 * time.Millisecond):
			}
			close(inner.release)
			if err := <-replaced; err != nil {
				t.Fatal(err)
			}
			if got := <-observed; got != want[r.name] {
				t.Errorf("%s observed %q after the replacement, want %q", r.name, got, want[r.name])
			}
		})
	}
}
