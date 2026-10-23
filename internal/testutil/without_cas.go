package testutil

import (
	"context"
	"io"
)

// withoutCAS embeds only the Store methods, so any CompareAndSwap of the
// wrapped store is hidden.
type withoutCAS struct{ Store }

// WithoutCAS returns store without its CompareAndSwap, as a backend that
// offers no conditional write would be. Tests use it to exercise the paths
// for such stores, for example lode.SingleProcess.
func WithoutCAS(store Store) Store {
	return withoutCAS{store}
}

// ReadRange forwards to the wrapped store, which keeps its range reads.
func (w withoutCAS) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return readRange(ctx, w.Store, path, offset, length)
}

// ReaderAt forwards to the wrapped store.
func (w withoutCAS) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	return readerAt(ctx, w.Store, path)
}
