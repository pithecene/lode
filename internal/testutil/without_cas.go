package testutil

// withoutCAS embeds only the Store methods, so any CompareAndSwap of the
// wrapped store is hidden.
type withoutCAS struct{ Store }

// WithoutCAS returns store without its CompareAndSwap, as a backend that
// offers no conditional write would be. Tests use it to exercise the paths
// for such stores, for example lode.SingleProcess.
// The result reads ranges exactly when store does.
func WithoutCAS(store Store) Store {
	if ranges, ok := store.(rangeReader); ok {
		return withoutCASRanges{withoutCAS{store}, ranges}
	}
	return withoutCAS{store}
}

// withoutCASRanges is withoutCAS over a store that reads ranges, which it
// keeps.
type withoutCASRanges struct {
	withoutCAS
	rangeReader
}
