package lode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pithecene-io/lode/internal/storepath"
)

// maxReadRangeLength is the largest ReadRange length a store accepts
// (storepath.MaxRangeLength): what a []byte can address on this platform.
const maxReadRangeLength = storepath.MaxRangeLength

// -----------------------------------------------------------------------------
// Filesystem Store
// -----------------------------------------------------------------------------

// fsStore implements Store using the local filesystem.
type fsStore struct {
	root string
}

// NewFSFactory returns a StoreFactory that creates a filesystem-backed Store.
// The directory must exist when the factory is invoked.
func NewFSFactory(root string) StoreFactory {
	return func() (Store, error) {
		return NewFS(root)
	}
}

// NewFS creates a filesystem-backed Store rooted at the given directory.
// The directory must exist.
//
// Consistency: Immediate read-after-write on local filesystems.
func NewFS(root string) (Store, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("lode: fs stat root: %w", err)
	}
	if !info.IsDir() {
		return nil, os.ErrNotExist
	}
	return &fsStore{root: root}, nil
}

func (f *fsStore) Put(_ context.Context, path string, r io.Reader) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}

	if _, err := os.Stat(fullPath); err == nil {
		return ErrPathExists
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("lode: fs put: %w", err)
	}

	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return ErrPathExists
		}
		return fmt.Errorf("lode: fs put: %w", err)
	}
	if _, err = io.Copy(file, r); err != nil {
		// Close before removing: an open file cannot be removed on every platform.
		_ = file.Close()
		return failPut(fullPath, err)
	}
	// A delayed write error surfaces at Close; the object is still incomplete.
	if err := file.Close(); err != nil {
		return failPut(fullPath, err)
	}
	return nil
}

// failPut finishes a Put that created the object and then failed: it removes
// the object so the path is as it was (CONTRACT_STORAGE §Put all-or-nothing)
// and returns the cause, joined with the removal error if that fails too.
// The caller never has to guess whether a failed Put left an object behind.
func failPut(fullPath string, cause error) error {
	wrapped := fmt.Errorf("lode: fs put: %w", cause)
	if rmErr := os.Remove(fullPath); rmErr != nil {
		return errors.Join(wrapped, fmt.Errorf("lode: fs put: remove partial object: %w", rmErr))
	}
	return wrapped
}

func (f *fsStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lode: fs get: %w", err)
	}
	return file, nil
}

func (f *fsStore) Exists(_ context.Context, path string) (bool, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("lode: fs exists: %w", err)
}

func (f *fsStore) List(_ context.Context, prefix string) ([]string, error) {
	searchPath, err := f.safePathForPrefix(prefix)
	if err != nil {
		return nil, err
	}

	// List is a string-prefix match over paths (CONTRACT_STORAGE §List), not a
	// directory listing: "a/b" matches both "a/b/x" and "a/bc", as it does on
	// the memory and S3 stores. A prefix that is empty or ends in "/" names a
	// directory and is walked directly; any other prefix is matched by walking
	// its parent directory and filtering, so a partial final token such as
	// "day=2026-10" works.
	walkRoot, filter := searchPath, ""
	if searchPath != f.root && !strings.HasSuffix(prefix, "/") {
		// A partial final token: walk the parent and keep string-prefix matches.
		// Prefixes that normalize to the root ("", ".", "a/..") are a direct
		// root walk; never step above f.root to find a parent.
		walkRoot = filepath.Dir(searchPath)
		filter = filepath.ToSlash(filepath.Clean(prefix))
	}

	// Return empty list if the directory doesn't exist (empty dataset/prefix
	// semantics). A directory walk root that is a regular file also matches
	// nothing: the object "a" does not start with the prefix "a/".
	info, err := os.Stat(walkRoot)
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return nil, nil
	}

	var paths []string

	err = filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("lode: fs list: %w", err)
		}
		if d.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(f.root, path)
		if err != nil {
			return fmt.Errorf("lode: fs list: %w", err)
		}
		rel := filepath.ToSlash(relPath)
		if filter != "" && !strings.HasPrefix(rel, filter) {
			return nil
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func (f *fsStore) Delete(_ context.Context, path string) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}
	err = os.Remove(fullPath)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lode: fs delete: %w", err)
	}
	return nil
}

func (f *fsStore) ReadRange(_ context.Context, path string, offset, length int64) ([]byte, error) {
	if !storepath.RangeValid(offset, length) {
		return nil, ErrInvalidPath
	}

	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lode: fs read range: %w", err)
	}
	defer closer(file)()

	// Bound the buffer by what the file can supply: a range past EOF returns
	// the available bytes (CONTRACT_STORAGE §ReadRange), so a large valid
	// length must not allocate memory the read can never fill.
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("lode: fs read range: %w", err)
	}
	available := info.Size() - offset
	if available <= 0 {
		return []byte{}, nil
	}
	length = min(length, available)

	data := make([]byte, int(length))
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("lode: fs read range: %w", err)
	}

	return data[:n], nil
}

func (f *fsStore) ReaderAt(_ context.Context, path string) (io.ReaderAt, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lode: fs reader at: %w", err)
	}

	// Note: The caller is responsible for closing via type assertion if needed.
	// *os.File implements io.ReaderAt.
	return file, nil
}

// safePathForFile resolves an object key to a filesystem path inside root.
// The key rules are the shared ones (storepath.Key); an absolute key is
// refused outright rather than re-rooted, and the result must lie under root.
func (f *fsStore) safePathForFile(path string) (string, error) {
	key, ok := storepath.Key(path)
	if !ok || filepath.IsAbs(path) {
		return "", ErrInvalidPath
	}
	return f.resolve(key, false)
}

// safePathForPrefix resolves a listing prefix to the directory to walk; the
// root itself for an empty or root-equivalent prefix.
func (f *fsStore) safePathForPrefix(prefix string) (string, error) {
	rel, ok := storepath.Prefix(prefix)
	if !ok || filepath.IsAbs(prefix) {
		return "", ErrInvalidPath
	}
	return f.resolve(rel, true)
}

// resolve joins rel beneath root and refuses anything that resolves outside
// it. The shared rules clean "/"-separated keys; this check is the
// filesystem's own defense, catching a traversal spelled with the platform
// separator. allowRoot admits rel "" (a listing of everything).
func (f *fsStore) resolve(rel string, allowRoot bool) (string, error) {
	full := filepath.Join(f.root, filepath.FromSlash(rel))
	absRoot, err := filepath.Abs(f.root)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if absFull == absRoot {
		if !allowRoot {
			return "", ErrInvalidPath
		}
		return f.root, nil
	}
	if !strings.HasPrefix(absFull, absRoot+string(filepath.Separator)) {
		return "", ErrInvalidPath
	}
	return full, nil
}

// -----------------------------------------------------------------------------
// Memory Store
// -----------------------------------------------------------------------------

// memoryStore implements Store using an in-memory map.
type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryFactory returns a StoreFactory that creates an in-memory Store.
func NewMemoryFactory() StoreFactory {
	return func() (Store, error) {
		return NewMemory(), nil
	}
}

// NewMemory creates an in-memory Store.
//
// Consistency: Immediate.
// Memory is safe for concurrent use.
func NewMemory() Store {
	return &memoryStore{
		data: make(map[string][]byte),
	}
}

func (m *memoryStore) Put(_ context.Context, path string, r io.Reader) error {
	normalized, valid := storepath.Key(path)
	if !valid {
		return ErrInvalidPath
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[normalized]; exists {
		return ErrPathExists
	}

	m.data[normalized] = data
	return nil
}

func (m *memoryStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	normalized, valid := storepath.Key(path)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	data, exists := m.data[normalized]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return io.NopCloser(strings.NewReader(string(dataCopy))), nil
}

func (m *memoryStore) Exists(_ context.Context, path string) (bool, error) {
	normalized, valid := storepath.Key(path)
	if !valid {
		return false, ErrInvalidPath
	}

	m.mu.RLock()
	_, exists := m.data[normalized]
	m.mu.RUnlock()

	return exists, nil
}

func (m *memoryStore) List(_ context.Context, prefix string) ([]string, error) {
	normalized, valid := storepath.Prefix(prefix)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var paths []string
	for path := range m.data {
		if strings.HasPrefix(path, normalized) {
			paths = append(paths, path)
		}
	}

	return paths, nil
}

func (m *memoryStore) Delete(_ context.Context, path string) error {
	normalized, valid := storepath.Key(path)
	if !valid {
		return ErrInvalidPath
	}

	m.mu.Lock()
	delete(m.data, normalized)
	m.mu.Unlock()

	return nil
}

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See ConditionalWriter for semantics.
func (m *memoryStore) CompareAndSwap(_ context.Context, path, expected, replacement string) error {
	normalized, valid := storepath.Key(path)
	if !valid {
		return ErrInvalidPath
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	data, exists := m.data[normalized]
	switch {
	case !exists && expected == "":
		m.data[normalized] = []byte(replacement)
		return nil
	case !exists:
		return ErrSnapshotConflict
	case string(data) == expected:
		m.data[normalized] = []byte(replacement)
		return nil
	default:
		return ErrSnapshotConflict
	}
}

func (m *memoryStore) ReadRange(_ context.Context, path string, offset, length int64) ([]byte, error) {
	if !storepath.RangeValid(offset, length) {
		return nil, ErrInvalidPath
	}

	normalized, valid := storepath.Key(path)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	data, exists := m.data[normalized]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	// Bounds checking
	if offset >= int64(len(data)) {
		return []byte{}, nil
	}

	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	result := make([]byte, int(end-offset))
	copy(result, data[offset:end])
	return result, nil
}

func (m *memoryStore) ReaderAt(_ context.Context, path string) (io.ReaderAt, error) {
	normalized, valid := storepath.Key(path)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	data, exists := m.data[normalized]
	m.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	// Return a bytes.Reader which implements io.ReaderAt
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return bytes.NewReader(dataCopy), nil
}
