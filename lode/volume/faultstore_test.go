package volume

import (
	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// memoryFactoryFrom returns a StoreFactory that hands out the given store, so
// several volumes can share one in-memory store.
func memoryFactoryFrom(store lode.RangeStore) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) { return store, nil }
}

// newFaultStoreFactory creates a StoreFactory that returns the given fault store.
func newFaultStoreFactory(fs *testutil.FaultStore) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) {
		return fs, nil
	}
}
