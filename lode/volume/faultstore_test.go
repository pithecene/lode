package volume

import (
	"github.com/pithecene-io/lode/internal/testutil"
	"github.com/pithecene-io/lode/lode"
)

// memoryFactoryFrom returns a StoreFactory that hands out the given store, so
// several volumes can share one in-memory store.
func memoryFactoryFrom(store lode.Store) lode.StoreFactory {
	return func() (lode.Store, error) { return store, nil }
}

// newFaultStoreFactory creates a StoreFactory that returns the given fault store.
func newFaultStoreFactory(fs *testutil.FaultStore) lode.StoreFactory {
	return func() (lode.Store, error) {
		return fs, nil
	}
}
