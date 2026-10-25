package testutil

import (
	"context"
	"io"
	"sync"
)

// rangeStore is the method set of lode.RangeStore, restated so this package
// does not import lode. Any lode.RangeStore satisfies it, and a *KeyRecorder
// satisfies lode.RangeStore.
type rangeStore interface {
	Store
	conditionalWriter
}

// KeyRecorder wraps a store and records the key or prefix of every call made
// during Recorded, so a test can check which objects a call reached.
type KeyRecorder struct {
	inner     rangeStore
	mu        sync.Mutex
	recording bool
	keys      []string
}

// NewKeyRecorder wraps inner, which must also implement CompareAndSwap.
func NewKeyRecorder(inner Store) *KeyRecorder {
	cw, ok := inner.(conditionalWriter)
	if !ok {
		panic("testutil: NewKeyRecorder needs a store with CompareAndSwap")
	}
	return &KeyRecorder{inner: struct {
		Store
		conditionalWriter
	}{inner, cw}}
}

// Recorded runs call and returns the keys and prefixes that it reached.
func (k *KeyRecorder) Recorded(call func()) []string {
	k.mu.Lock()
	k.recording, k.keys = true, nil
	k.mu.Unlock()
	call()
	k.mu.Lock()
	defer k.mu.Unlock()
	k.recording = false
	return k.keys
}

func (k *KeyRecorder) note(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.recording {
		k.keys = append(k.keys, key)
	}
}

// Put records path and delegates.
func (k *KeyRecorder) Put(ctx context.Context, path string, r io.Reader) error {
	k.note(path)
	return k.inner.Put(ctx, path, r)
}

// Get records path and delegates.
func (k *KeyRecorder) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	k.note(path)
	return k.inner.Get(ctx, path)
}

// Exists records path and delegates.
func (k *KeyRecorder) Exists(ctx context.Context, path string) (bool, error) {
	k.note(path)
	return k.inner.Exists(ctx, path)
}

// List records prefix and delegates.
func (k *KeyRecorder) List(ctx context.Context, prefix string) ([]string, error) {
	k.note(prefix)
	return k.inner.List(ctx, prefix)
}

// Delete records path and delegates.
func (k *KeyRecorder) Delete(ctx context.Context, path string) error {
	k.note(path)
	return k.inner.Delete(ctx, path)
}

// CompareAndSwap records path and delegates.
func (k *KeyRecorder) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	k.note(path)
	return k.inner.CompareAndSwap(ctx, path, expected, replacement)
}

// ReadRange records path and delegates.
func (k *KeyRecorder) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	k.note(path)
	return k.inner.ReadRange(ctx, path, offset, length)
}

// ReaderAt records path and delegates.
func (k *KeyRecorder) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	k.note(path)
	return k.inner.ReaderAt(ctx, path)
}
