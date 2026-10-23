package volume

import (
	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// memoryFactoryFrom returns a StoreFactory that hands out the given store, so
// several volumes can share one in-memory store.
func memoryFactoryFrom(store lode.Store) lode.Factory[lode.Store] {
	return func() (lode.Store, error) { return store, nil }
}

// noRanges hides a store's range reads, as a backend without them would.
type noRanges struct{ lode.Store }

// newFaultStoreFactory creates a StoreFactory that returns the given fault store.
func newFaultStoreFactory(fs *testutil.FaultStore) lode.Factory[lode.Store] {
	return func() (lode.Store, error) {
		return fs, nil
	}
}
