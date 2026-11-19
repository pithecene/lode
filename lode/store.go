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
	"slices"
	"strings"
	"sync"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// maxReadRangeLength is the largest ReadRange length a store accepts
// (storepath.MaxRangeLength): what a []byte can address on this platform.
const maxReadRangeLength = storepath.MaxRangeLength

// -----------------------------------------------------------------------------
// Filesystem Store
// -----------------------------------------------------------------------------

type fsConfig struct {
	sync bool
}

// FSOption configures a filesystem-backed store (FS / NewFS).
type FSOption interface {
	applyFS(*fsConfig) error
}

type fsOption func(*fsConfig) error

func (f fsOption) applyFS(cfg *fsConfig) error { return f(cfg) }

// WithSync fsyncs temporary object and pointer files (and their parent
// directory on publish) before Put or CompareAndSwap returns, providing
// power-loss durability in addition to crash-consistent atomicity.
func WithSync() FSOption {
	return fsOption(func(cfg *fsConfig) error {
		cfg.sync = true
		return nil
	})
}

func resolveFSConfig(opts []FSOption) (fsConfig, error) {
	var cfg fsConfig
	for _, opt := range opts {
		if opt == nil {
			return cfg, validate.Argument("fs option must not be nil")
		}
		if err := opt.applyFS(&cfg); err != nil {
			return cfg, err
		}
	}
	return cfg, nil
}

// fsStore implements RangeStore and PrefixLister using the local filesystem.
type fsStore struct {
	root string
	sync bool
}

var (
	_ RangeStore   = (*fsStore)(nil)
	_ PrefixLister = (*fsStore)(nil)
)

// FS returns a factory for a filesystem-backed RangeStore rooted at the
// given directory (NewFS). The directory must exist when a root opens; until
// then the factory performs no I/O.
func FS(root string, opts ...FSOption) Factory[RangeStore] {
	return func() (RangeStore, error) {
		return NewFS(root, opts...)
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
// Consistency: Immediate read-after-write on local filesystems. Pass WithSync
// when power-loss durability (fsync) is required in addition to atomic publish.
func NewFS(root string, opts ...FSOption) (RangeStore, error) {
	cfg, err := resolveFSConfig(opts)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("fs: stat root: %w", err)
	}
	if !info.IsDir() {
		return nil, os.ErrNotExist
	}
	return &fsStore{root: root, sync: cfg.sync}, nil
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
	existing, err := f.prepareDir(dir)
	if err != nil {
		return err
	}

	tmp, err := createTemp(dir)
	if err != nil {
		return fmt.Errorf("fs: put: %w", err)
	}
	if err := writeTemp(tmp, r, f.sync); err != nil {
		return discardTemp(tmp.Name(), err)
	}
	if err := os.Link(tmp.Name(), fullPath); err != nil {
		if os.IsExist(err) {
			return discardTemp(tmp.Name(), ErrPathExists)
		}
		return discardTemp(tmp.Name(), fmt.Errorf("fs: put: %w", err))
	}
	_ = os.Remove(tmp.Name())
	return f.syncPublishedDir(existing, dir)
}

func (f *fsStore) prepareDir(dir string) (string, error) {
	cleanDir := filepath.Clean(dir)
	var existing string
	if f.sync {
		var err error
		existing, err = deepestExistingDir(cleanDir, filepath.Clean(f.root))
		if err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(cleanDir, 0o755); err != nil {
		return "", fmt.Errorf("fs: mkdir: %w", err)
	}
	return existing, nil
}

func deepestExistingDir(dir, root string) (string, error) {
	for cur := dir; ; cur = filepath.Dir(cur) {
		_, err := os.Stat(cur)
		if err == nil {
			return cur, nil
		}
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("fs: stat dir: %w", err)
		}
		if cur == root || filepath.Dir(cur) == cur {
			return cur, nil
		}
	}
}

func (f *fsStore) syncPublishedDir(existing, dir string) error {
	if !f.sync {
		return nil
	}
	for cur := filepath.Clean(dir); ; cur = filepath.Dir(cur) {
		if err := syncDir(cur); err != nil {
			return err
		}
		if cur == existing || filepath.Dir(cur) == cur {
			return nil
		}
	}
}

func createTemp(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, tempPrefix+rand.Text()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
}

func writeTemp(tmp *os.File, r io.Reader, doSync bool) error {
	_, err := io.Copy(tmp, r)
	if err == nil && doSync {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("fs: put: %w", err)
	}
	return nil
}

func discardTemp(name string, cause error) error {
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return errors.Join(cause, fmt.Errorf("fs: put: remove temporary file: %w", err))
	}
	return cause
}

const tempPrefix = storepath.ReservedPrefix

func isTemporary(name string) bool {
	return storepath.Reserved(filepath.ToSlash(name))
}

func (f *fsStore) openObject(path, op string) (*os.File, error) {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return nil, err
	}
	file, err := openRead(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("fs: %s: %w", op, err)
	}
	return file, nil
}

func (f *fsStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	return f.openObject(path, "get")
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

func (f *fsStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	searchPath, err := f.safePathForPrefix(prefix)
	if err != nil {
		return nil, err
	}

	walkRoot, filter := searchPath, ""
	if searchPath != f.root && !strings.HasSuffix(prefix, "/") {
		walkRoot = filepath.Dir(searchPath)
		filter = filepath.ToSlash(filepath.Clean(prefix))
	}

	info, err := os.Stat(walkRoot)
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return nil, nil
	}

	var paths []string
	err = filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
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

// ListPrefixes returns immediate child prefixes (each ending in "/") beneath
// prefix that contain at least one stored object, sorted lexicographically.
func (f *fsStore) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, ok := storepath.Prefix(prefix)
	if !ok || isRooted(prefix) {
		return nil, ErrInvalidPath
	}

	var parentDir, parentPrefix, partial string
	var err error
	switch {
	case normalized == "":
		parentDir = f.root
	case strings.HasSuffix(normalized, "/"):
		dirRel := strings.TrimSuffix(normalized, "/")
		parentDir, err = f.resolve(dirRel, false)
		if err != nil {
			return nil, err
		}
		parentPrefix = normalized
	default:
		if slash := strings.LastIndexByte(normalized, '/'); slash >= 0 {
			dirRel := normalized[:slash]
			parentDir, err = f.resolve(dirRel, false)
			if err != nil {
				return nil, err
			}
			parentPrefix = dirRel + "/"
			partial = normalized[slash+1:]
		} else {
			parentDir = f.root
			partial = normalized
		}
	}

	info, statErr := os.Stat(parentDir)
	if os.IsNotExist(statErr) || (statErr == nil && !info.IsDir()) {
		return []string{}, nil
	}
	if statErr != nil {
		return nil, fmt.Errorf("fs: list prefixes: %w", statErr)
	}

	entries, err := os.ReadDir(parentDir)
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fs: list prefixes: %w", err)
	}

	prefixes := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || isTemporary(name) || (partial != "" && !strings.HasPrefix(name, partial)) {
			continue
		}
		hasObj, err := hasAnyObject(filepath.Join(parentDir, name))
		if err != nil {
			return nil, err
		}
		if hasObj {
			prefixes = append(prefixes, parentPrefix+name+"/")
		}
	}
	return prefixes, nil
}

// hasAnyObject reports whether dir contains at least one published object,
// returning on the first regular non-temporary file encountered.
func hasAnyObject(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("fs: list prefixes: %w", err)
	}
	for _, e := range entries {
		if isTemporary(e.Name()) {
			continue
		}
		if !e.IsDir() {
			return true, nil
		}
		ok, err := hasAnyObject(filepath.Join(dir, e.Name()))
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
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
	return f.openObject(path, "reader at")
}

func (f *fsStore) safePathForFile(path string) (string, error) {
	key, ok := storepath.Key(path)
	if !ok || isRooted(path) {
		return "", ErrInvalidPath
	}
	return f.resolve(key, false)
}

func (f *fsStore) safePathForPrefix(prefix string) (string, error) {
	rel, ok := storepath.Prefix(prefix)
	if !ok || isRooted(prefix) {
		return "", ErrInvalidPath
	}
	return f.resolve(rel, true)
}

func isRooted(p string) bool {
	return strings.HasPrefix(p, "/") || filepath.IsAbs(p)
}

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

// memoryStore implements RangeStore and PrefixLister using an in-memory map.
type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

var (
	_ RangeStore   = (*memoryStore)(nil)
	_ PrefixLister = (*memoryStore)(nil)
)

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

func (m *memoryStore) bytesCopy(path string) ([]byte, error) {
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
	return bytes.Clone(data), nil
}

func (m *memoryStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	b, err := m.bytesCopy(path)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
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

// ListPrefixes returns immediate child prefixes (each ending in "/") beneath
// prefix, sorted lexicographically.
func (m *memoryStore) ListPrefixes(_ context.Context, prefix string) ([]string, error) {
	normalized, valid := storepath.Prefix(prefix)
	if !valid {
		return nil, ErrInvalidPath
	}

	m.mu.RLock()
	seen := make(map[string]struct{})
	for path := range m.data {
		if !strings.HasPrefix(path, normalized) {
			continue
		}
		rest := path[len(normalized):]
		idx := strings.IndexByte(rest, '/')
		if idx < 0 {
			continue
		}
		seen[normalized+rest[:idx+1]] = struct{}{}
	}
	m.mu.RUnlock()

	prefixes := make([]string, 0, len(seen))
	for p := range seen {
		prefixes = append(prefixes, p)
	}
	slices.Sort(prefixes)
	return prefixes, nil
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
	b, err := m.bytesCopy(path)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}
