package testutil

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
)

// -----------------------------------------------------------------------------
// Fault-Injection Store Wrapper (test-only)
// -----------------------------------------------------------------------------
//
// FaultStore wraps a Store and enables deterministic fault injection for testing
// streaming failure paths. It provides:
//   - Error injection on specific operations
//   - Call observation/recording
//   - Blocking/synchronization points
//
// This is NOT production code. It exists solely to verify contract guarantees
// under controlled failure conditions without relying on timing.

// Store is the method set of lode.Store, restated so this package does not
// import lode (lode's own tests use it). Any lode.Store satisfies it, and a
// *FaultStore satisfies lode.Store and lode.RangeReader.
type Store interface {
	Put(ctx context.Context, path string, r io.Reader) error
	Get(ctx context.Context, path string) (io.ReadCloser, error)
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, path string) error
}

// rangeReader mirrors lode.RangeReader.
type rangeReader interface {
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)
	ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}

// conditionalWriter mirrors lode.ConditionalWriter.
type conditionalWriter interface {
	CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

// FaultStore wraps a Store with fault injection capabilities.
type FaultStore struct {
	inner  Store
	ranges rangeReader // inner's range reads; nil if it has none

	mu sync.Mutex

	// Error injection: set these to inject errors on specific operations
	putErr      error
	putErrMatch string // if set, only inject putErr on paths containing this substring
	getErr      error
	getErrMatch string // if set, only inject getErr on paths containing this substring
	deleteErr   error
	existsErr   error
	listErr     error

	// Call observation: tracks which methods were called
	putCalls    []string
	getCalls    []string
	deleteCalls []string
	existsCalls []string
	listCalls   []string

	// Blocking: channels to synchronize test assertions with operations
	putBlock    chan struct{} // if non-nil, Put blocks until closed
	deleteBlock chan struct{} // if non-nil, Delete blocks until closed

	// Pre-call hooks: called after recording but before blocking
	beforePut func(path string)

	// Post-call hooks: called after operation completes (before returning)
	afterPut    func(path string, err error)
	afterDelete func(path string, err error)

	// CAS fault injection (CompareAndSwap)
	casErr      error
	casErrMatch string
	casCalls    []string
	casBlock    chan struct{}
	beforeCAS   func(path string)
}

// NewFaultStore creates a fault-injection wrapper around the given store.
// Its ReadRange and ReaderAt require an inner store that reads ranges, as
// every built-in store does.
func NewFaultStore(inner Store) *FaultStore {
	ranges, _ := inner.(rangeReader)
	return &FaultStore{inner: inner, ranges: ranges}
}

// --- Fault injection setters ---

// SetPutError sets an error to be returned by Put calls.
// If match is non-empty, error is only returned for paths containing match.
func (f *FaultStore) SetPutError(err error, match ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putErr = err
	if len(match) > 0 {
		f.putErrMatch = match[0]
	} else {
		f.putErrMatch = ""
	}
}

// SetGetError sets an error to be returned by Get calls.
// If match is non-empty, error is only returned for paths containing match.
func (f *FaultStore) SetGetError(err error, match ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getErr = err
	if len(match) > 0 {
		f.getErrMatch = match[0]
	} else {
		f.getErrMatch = ""
	}
}

// SetDeleteError sets an error to be returned by all Delete calls.
func (f *FaultStore) SetDeleteError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteErr = err
}

// SetPutBlock sets a channel that Put will block on before proceeding.
// Close the channel to unblock.
func (f *FaultStore) SetPutBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putBlock = ch
}

// SetBeforePut sets a hook called after recording but before blocking.
// Use this for deterministic synchronization with blocking operations.
func (f *FaultStore) SetBeforePut(hook func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforePut = hook
}

// SetAfterPut sets a hook called after each Put completes.
func (f *FaultStore) SetAfterPut(hook func(path string, err error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterPut = hook
}

// SetAfterDelete sets a hook called after each Delete completes.
func (f *FaultStore) SetAfterDelete(hook func(path string, err error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.afterDelete = hook
}

// SetCASError sets an error to be returned by CompareAndSwap calls.
// If match is non-empty, error is only returned for paths containing match.
func (f *FaultStore) SetCASError(err error, match ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casErr = err
	if len(match) > 0 {
		f.casErrMatch = match[0]
	} else {
		f.casErrMatch = ""
	}
}

// SetCASBlock sets a channel that CompareAndSwap will block on before proceeding.
// Close the channel to unblock.
func (f *FaultStore) SetCASBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.casBlock = ch
}

// SetBeforeCAS sets a hook called after recording but before blocking.
func (f *FaultStore) SetBeforeCAS(hook func(path string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeCAS = hook
}

// --- Call observation ---

// PutCalls returns paths passed to Put.
func (f *FaultStore) PutCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.putCalls...)
}

// DeleteCalls returns paths passed to Delete.
func (f *FaultStore) DeleteCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleteCalls...)
}

// ListCalls returns prefixes passed to List.
func (f *FaultStore) ListCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listCalls...)
}

// GetCalls returns paths passed to Get.
func (f *FaultStore) GetCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.getCalls...)
}

// CASCalls returns paths passed to CompareAndSwap.
func (f *FaultStore) CASCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.casCalls...)
}

// Reset clears all recorded calls.
func (f *FaultStore) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls = nil
	f.getCalls = nil
	f.deleteCalls = nil
	f.existsCalls = nil
	f.listCalls = nil
	f.casCalls = nil
}

// --- Store interface implementation ---

// Put records the call, runs the put hooks and block, returns a matching
// injected error, and otherwise forwards to the inner store.
func (f *FaultStore) Put(ctx context.Context, path string, r io.Reader) error {
	f.mu.Lock()
	injectedErr := f.putErr
	errMatch := f.putErrMatch
	block := f.putBlock
	beforeHook := f.beforePut
	hook := f.afterPut
	f.putCalls = append(f.putCalls, path)
	f.mu.Unlock()

	// Call beforePut hook (for deterministic sync before blocking)
	if beforeHook != nil {
		beforeHook(path)
	}

	// Block if configured (useful for testing cancellation windows)
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Return injected error if set (with optional path matching)
	if injectedErr != nil && (errMatch == "" || strings.Contains(path, errMatch)) {
		if hook != nil {
			hook(path, injectedErr)
		}
		return injectedErr
	}

	// Delegate to inner store
	err := f.inner.Put(ctx, path, r)
	if hook != nil {
		hook(path, err)
	}
	return err
}

// Get records the call and forwards unless a get error is injected.
func (f *FaultStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	f.mu.Lock()
	injectedErr := f.getErr
	errMatch := f.getErrMatch
	f.getCalls = append(f.getCalls, path)
	f.mu.Unlock()

	if injectedErr != nil && (errMatch == "" || strings.Contains(path, errMatch)) {
		return nil, injectedErr
	}
	return f.inner.Get(ctx, path)
}

// Exists records the call and forwards unless an exists error is injected.
func (f *FaultStore) Exists(ctx context.Context, path string) (bool, error) {
	f.mu.Lock()
	injectedErr := f.existsErr
	f.existsCalls = append(f.existsCalls, path)
	f.mu.Unlock()

	if injectedErr != nil {
		return false, injectedErr
	}
	return f.inner.Exists(ctx, path)
}

// List records the call and forwards unless a list error is injected.
func (f *FaultStore) List(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	injectedErr := f.listErr
	f.listCalls = append(f.listCalls, prefix)
	f.mu.Unlock()

	if injectedErr != nil {
		return nil, injectedErr
	}
	return f.inner.List(ctx, prefix)
}

// Delete records the call, runs the delete block and hook, and forwards
// unless a delete error is injected.
func (f *FaultStore) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	injectedErr := f.deleteErr
	block := f.deleteBlock
	hook := f.afterDelete
	f.deleteCalls = append(f.deleteCalls, path)
	f.mu.Unlock()

	// Block if configured
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if injectedErr != nil {
		if hook != nil {
			hook(path, injectedErr)
		}
		return injectedErr
	}

	err := f.inner.Delete(ctx, path)
	if hook != nil {
		hook(path, err)
	}
	return err
}

// ReadRange forwards to the inner store.
func (f *FaultStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return f.ranges.ReadRange(ctx, path, offset, length)
}

// ReaderAt forwards to the inner store.
func (f *FaultStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	return f.ranges.ReaderAt(ctx, path)
}

// CompareAndSwap forwards to the inner store's ConditionalWriter implementation
// with full fault injection support (error injection, blocking, hooks).
func (f *FaultStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	f.mu.Lock()
	injectedErr := f.casErr
	errMatch := f.casErrMatch
	block := f.casBlock
	beforeHook := f.beforeCAS
	f.casCalls = append(f.casCalls, path)
	f.mu.Unlock()

	if beforeHook != nil {
		beforeHook(path)
	}

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if injectedErr != nil && (errMatch == "" || strings.Contains(path, errMatch)) {
		return injectedErr
	}

	return f.inner.(conditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}

// --- Sentinel errors for injection ---

var (
	// ErrInjectedPut is a ready-made error for SetPutError.
	ErrInjectedPut = errors.New("injected: put error")
	// ErrInjectedDelete is a ready-made error for SetDeleteError.
	ErrInjectedDelete = errors.New("injected: delete error")
)
