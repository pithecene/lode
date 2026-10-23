package lode

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pithecene/lode/internal/storepath"
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

// FS returns a factory for a filesystem-backed RangeStore rooted at the
// given directory (NewFS). The directory must exist when a root opens; until
// then the factory performs no I/O.
func FS(root string) Factory[RangeStore] {
	return func() (RangeStore, error) {
		return NewFS(root)
	}
}

// Use returns a factory for a store the caller already holds, so that
// several roots, or a root and code outside Lode, share one store value:
// lode.Open(lode.Use(store)). Sharing matters for a store that coordinates
// only within one value, such as SingleProcess.
func Use[S ObjectStore](store S) Factory[S] {
	return func() (S, error) {
		return store, nil
	}
}

// NewFS creates a filesystem-backed RangeStore rooted at the given directory.
// The directory must exist, on a filesystem that supports hard links: Put
// publishes each object with one, and fails where links are unsupported
// (CONTRACT_STORAGE §Put).
//
// Consistency: Immediate read-after-write on local filesystems.
func NewFS(root string) (RangeStore, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("fs: stat root: %w", err)
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
		return fmt.Errorf("fs: put: %w", err)
	}

	// The object is written under a temporary name and then published with a
	// hard link, which fails if the path exists: a concurrent reader sees the
	// whole object or none of it (CONTRACT_STORAGE §Put), and a concurrent
	// Put cannot overwrite it. List never reports a temporary name.
	tmp, err := createTemp(dir)
	if err != nil {
		return fmt.Errorf("fs: put: %w", err)
	}
	if err := writeTemp(tmp, r); err != nil {
		return discardTemp(tmp.Name(), err)
	}
	if err := os.Link(tmp.Name(), fullPath); err != nil {
		if os.IsExist(err) {
			return discardTemp(tmp.Name(), ErrPathExists)
		}
		return discardTemp(tmp.Name(), fmt.Errorf("fs: put: %w", err))
	}
	// The object is published. The temporary name is never listed and is
	// rejected as a key, so failing to remove it changes nothing a caller
	// can observe.
	_ = os.Remove(tmp.Name())
	return nil
}

// createTemp creates a temporary file with a random name in dir. It is
// created with mode 0644 under the process umask, as an object created at
// its final path would be, and the hard link that publishes it keeps that
// mode. A name already taken fails rather than being reused.
func createTemp(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, tempPrefix+rand.Text()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
}

// writeTemp copies r into the temporary file and closes it. A delayed write
// error surfaces at Close, and the object is then still incomplete.
func writeTemp(tmp *os.File, r io.Reader) error {
	_, err := io.Copy(tmp, r)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("fs: put: %w", err)
	}
	return nil
}

// discardTemp removes the temporary file of a failed Put and returns cause,
// joined with the removal failure if there is one (CONTRACT_STORAGE §Put).
func discardTemp(name string, cause error) error {
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return errors.Join(cause, fmt.Errorf("fs: put: remove temporary file: %w", err))
	}
	return cause
}

// tempPrefix starts the name of every temporary file the filesystem store
// creates while writing an object. The shared key rules reserve it, so no
// object key names such a file, and List omits them.
const tempPrefix = storepath.ReservedPrefix

// isTemporary reports whether a listed file is a temporary file of an
// object still being written.
func isTemporary(name string) bool {
	return storepath.Reserved(filepath.ToSlash(name))
}

func (f *fsStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}
	file, err := openRead(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("fs: get: %w", err)
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
	return false, fmt.Errorf("fs: exists: %w", err)
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
			return fmt.Errorf("fs: list: %w", err)
		}
		if d.IsDir() || isTemporary(path) {
			return nil
		}
		relPath, err := filepath.Rel(f.root, path)
		if err != nil {
			return fmt.Errorf("fs: list: %w", err)
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
		return fmt.Errorf("fs: delete: %w", err)
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

	file, err := openRead(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("fs: read range: %w", err)
	}
	defer closer(file)()

	// Bound the buffer by what the file can supply: a range past EOF returns
	// the available bytes (CONTRACT_STORAGE §ReadRange), so a large valid
	// length must not allocate memory the read can never fill.
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("fs: read range: %w", err)
	}
	available := info.Size() - offset
	if available <= 0 {
		return []byte{}, nil
	}
	length = min(length, available)

	data := make([]byte, int(length))
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("fs: read range: %w", err)
	}

	return data[:n], nil
}

func (f *fsStore) ReaderAt(_ context.Context, path string) (io.ReaderAt, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}

	file, err := openRead(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("fs: reader at: %w", err)
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
	if !ok || isRooted(path) {
		return "", ErrInvalidPath
	}
	return f.resolve(key, false)
}

// safePathForPrefix resolves a listing prefix to the directory to walk; the
// root itself for an empty or root-equivalent prefix.
func (f *fsStore) safePathForPrefix(prefix string) (string, error) {
	rel, ok := storepath.Prefix(prefix)
	if !ok || isRooted(prefix) {
		return "", ErrInvalidPath
	}
	return f.resolve(rel, true)
}

// isRooted reports whether p is absolute on any platform: a leading "/" or,
// on Windows, a volume such as "C:/". filepath.IsAbs alone misses the first
// on Windows, where "/etc" lacks a volume and is therefore not absolute.
func isRooted(p string) bool {
	return strings.HasPrefix(p, "/") || filepath.IsAbs(p)
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

// Memory returns a factory for a new in-memory RangeStore (NewMemory). Each
// root opened with it gets its own store; use Use(NewMemory()) to share one.
func Memory() Factory[RangeStore] {
	return func() (RangeStore, error) {
		return NewMemory(), nil
	}
}

// NewMemory creates an in-memory RangeStore.
//
// Consistency: Immediate.
// Memory is safe for concurrent use.
func NewMemory() RangeStore {
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
		return fmt.Errorf("memory: put: %w", err)
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
