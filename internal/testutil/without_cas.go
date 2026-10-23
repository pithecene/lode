package testutil

// withoutCAS embeds only the Store methods, so any CompareAndSwap of the
// wrapped store is hidden.
type withoutCAS struct{ Store }

// WithoutCAS returns store without its CompareAndSwap, as a backend that
// offers no conditional write would be. Tests use it to exercise the paths
// for such stores, for example lode.SingleProcess.
func WithoutCAS(store Store) Store {
	return withoutCAS{store}
}
