package lode

import "github.com/pithecene-io/lode/internal/testutil"

// The fault-injection store lives in internal/testutil so every package's
// tests can share it; these names keep this package's call sites short.

type faultStore = testutil.FaultStore

var (
	errInjectedPut    = testutil.ErrInjectedPut
	errInjectedDelete = testutil.ErrInjectedDelete
)

func newFaultStore(inner Store) *faultStore { return testutil.NewFaultStore(inner) }

// newFaultStoreFactory creates a StoreFactory that returns the given faultStore.
func newFaultStoreFactory(fs *faultStore) StoreFactory {
	return func() (Store, error) {
		return fs, nil
	}
}
