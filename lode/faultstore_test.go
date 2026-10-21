package lode

import "github.com/pithecene/lode/internal/testutil"

// newFaultStoreFactory creates a StoreFactory that returns the given fault store.
func newFaultStoreFactory(fs *testutil.FaultStore) StoreFactory {
	return func() (Store, error) {
		return fs, nil
	}
}
