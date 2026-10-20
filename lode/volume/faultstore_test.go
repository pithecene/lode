package volume

import (
	"github.com/pithecene-io/lode/internal/testutil"
	"github.com/pithecene-io/lode/lode"
)

// The fault-injection store is shared with lode's tests (internal/testutil);
// these names match the ones those tests use.

type faultStore = testutil.FaultStore

var errInjectedPut = testutil.ErrInjectedPut

func newFaultStore(inner lode.Store) *faultStore { return testutil.NewFaultStore(inner) }

// memoryFactoryFrom returns a StoreFactory that hands out the given store, so
// several volumes can share one in-memory store.
func memoryFactoryFrom(store lode.Store) lode.StoreFactory {
	return func() (lode.Store, error) { return store, nil }
}

// newFaultStoreFactory creates a StoreFactory that returns the given faultStore.
func newFaultStoreFactory(fs *faultStore) lode.StoreFactory {
	return func() (lode.Store, error) {
		return fs, nil
	}
}
