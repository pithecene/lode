package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// casJournalSuffix names the companion backup object written before Delete in
// SingleProcess.CompareAndSwap so a failed replacement Put or process crash
// never leaves the committed pointer unrecoverable.
const casJournalSuffix = ".lode-cas"

func isCASJournal(path string) bool {
	return strings.HasSuffix(path, casJournalSuffix)
}

func casJournalPath(path string) string {
	return path + casJournalSuffix
}

func casTargetPath(journal string) (string, bool) {
	return strings.CutSuffix(journal, casJournalSuffix)
}

// SingleProcess returns a Store whose CompareAndSwap serializes updates
// through a lock held by this process. It is the explicit escape hatch for a
// backend that offers no conditional write.
//
// CAS through the returned store is correct only while every writer to the
// store runs in this process and uses this one returned value. Writers in
// other processes, or a second SingleProcess wrapping the same backend, are
// not serialized and can fork a history.
//
// Before deleting an existing target, CompareAndSwap journals the verified
// prior content at <path>.lode-cas, restoring <path> if the replacement Put
// fails and serving <path>.lode-cas whenever <path> is absent so neither a
// replacement failure nor process termination loses the committed head.
//
// Reads through the returned store wait for a replacement in progress, so a
// reader in this process observes the old or the new content of a path,
// never its absence between the Delete and the Put. Optional capabilities of
// store (RangeReader, PrefixLister) are preserved on the returned value.
//
// A store that already implements ConditionalWriter is returned unchanged,
// and a nil or typed-nil store is returned as nil, so that a constructor
// rejects it.
func SingleProcess(store ObjectStore) Store {
	if validate.Nil(store) {
		return nil
	}
	if s, ok := store.(Store); ok {
		return s
	}
	base := &singleProcessStore{
		ObjectStore: store,
		mu:          newSingleProcessLock(),
		busy:        make(map[string]chan struct{}),
		journaled:   make(map[string]struct{}),
	}
	ranges, hasRanges := store.(RangeReader)
	prefixes, hasPrefixes := store.(PrefixLister)
	switch {
	case hasRanges && hasPrefixes:
		return singleProcessFullStore{
			singleProcessRangeStore: singleProcessRangeStore{singleProcessStore: base, ranges: ranges},
			prefixes:                prefixes,
		}
	case hasRanges:
		return singleProcessRangeStore{singleProcessStore: base, ranges: ranges}
	case hasPrefixes:
		return singleProcessPrefixStore{singleProcessStore: base, prefixes: prefixes}
	default:
		return base
	}
}

// singleProcessStore implements CompareAndSwap over a store without one.
// CompareAndSwap and Delete hold mu exclusively across pointer/object removal;
// reads hold mu shared; Put serializes per key via busy without holding mu
// while consuming caller-supplied readers.
type singleProcessStore struct {
	ObjectStore
	mu        singleProcessLock
	stateMu   sync.Mutex
	busy      map[string]chan struct{}
	journaled map[string]struct{}
}

// singleProcessLock admits one writer at a time. An admitted writer blocks
// new readers, and can give up its slot while waiting for existing readers.
type singleProcessLock struct {
	writerSlot  chan struct{}
	mu          sync.Mutex
	readers     int
	writing     bool
	readersDone chan struct{}
	writerDone  chan struct{}
}

func newSingleProcessLock() singleProcessLock {
	return singleProcessLock{writerSlot: make(chan struct{}, 1)}
}

func (m *singleProcessLock) Lock(ctx context.Context) error {
	select {
	case m.writerSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.mu.Lock()
	m.writing = true
	if m.readers != 0 {
		m.readersDone = make(chan struct{})
	}
	readersDone := m.readersDone
	m.mu.Unlock()

	if readersDone != nil {
		select {
		case <-readersDone:
		case <-ctx.Done():
		}
	}
	if err := ctx.Err(); err != nil {
		m.Unlock()
		return err
	}
	return nil
}

func (m *singleProcessLock) Unlock() {
	m.mu.Lock()
	m.writing = false
	m.readersDone = nil
	if m.writerDone != nil {
		close(m.writerDone)
		m.writerDone = nil
	}
	m.mu.Unlock()
	<-m.writerSlot
}

func (m *singleProcessLock) RLock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for m.writing {
		if m.writerDone == nil {
			m.writerDone = make(chan struct{})
		}
		writerDone := m.writerDone
		m.mu.Unlock()
		<-writerDone
		m.mu.Lock()
	}
	m.readers++
}

func (m *singleProcessLock) RUnlock() {
	m.mu.Lock()
	m.readers--
	if m.readers == 0 && m.readersDone != nil {
		close(m.readersDone)
		m.readersDone = nil
	}
	m.mu.Unlock()
}

func (s *singleProcessStore) acquireKey(ctx context.Context, key string) (chan struct{}, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.stateMu.Lock()
		waitCh, active := s.busy[key]
		if !active {
			ch := make(chan struct{})
			s.busy[key] = ch
			s.stateMu.Unlock()
			return ch, nil
		}
		s.stateMu.Unlock()
		select {
		case <-waitCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *singleProcessStore) releaseKey(key string, ch chan struct{}) {
	s.stateMu.Lock()
	delete(s.busy, key)
	close(ch)
	s.stateMu.Unlock()
}

func (s *singleProcessStore) setJournaled(key string, v bool) {
	s.stateMu.Lock()
	if v {
		s.journaled[key] = struct{}{}
	} else {
		delete(s.journaled, key)
	}
	s.stateMu.Unlock()
}

func (s *singleProcessStore) isJournaled(key string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	_, ok := s.journaled[key]
	return ok
}

func (s *singleProcessStore) checkPutPreconditions(ctx context.Context, key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isJournaled(key) {
		return ErrPathExists
	}
	exists, err := s.ObjectStore.Exists(ctx, key)
	if err != nil {
		return err
	}
	if exists {
		return ErrPathExists
	}
	jExists, err := s.ObjectStore.Exists(ctx, casJournalPath(key))
	if err != nil {
		return err
	}
	if jExists {
		s.setJournaled(key, true)
		return ErrPathExists
	}
	return nil
}

func (s *singleProcessStore) Put(ctx context.Context, path string, r io.Reader) error {
	cleaned, valid := storepath.Key(path)
	if !valid || isCASJournal(cleaned) {
		return ErrInvalidPath
	}
	ch, err := s.acquireKey(ctx, cleaned)
	if err != nil {
		return err
	}
	defer s.releaseKey(cleaned, ch)

	if err := s.checkPutPreconditions(ctx, cleaned); err != nil {
		return err
	}
	return s.ObjectStore.Put(ctx, cleaned, r)
}

func (s *singleProcessStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if isCASJournal(path) {
		return nil, ErrInvalidPath
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rc, err := s.ObjectStore.Get(ctx, path)
	if !errors.Is(err, ErrNotFound) {
		return rc, err
	}
	jrc, jerr := s.ObjectStore.Get(ctx, casJournalPath(path))
	if errors.Is(jerr, ErrNotFound) {
		return nil, err
	}
	return jrc, jerr
}

func (s *singleProcessStore) Exists(ctx context.Context, path string) (bool, error) {
	if isCASJournal(path) {
		return false, ErrInvalidPath
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ok, err := s.ObjectStore.Exists(ctx, path)
	if err != nil || ok {
		return ok, err
	}
	return s.ObjectStore.Exists(ctx, casJournalPath(path))
}

func (s *singleProcessStore) Delete(ctx context.Context, path string) error {
	cleaned, valid := storepath.Key(path)
	if !valid || isCASJournal(cleaned) {
		return ErrInvalidPath
	}
	ch, err := s.acquireKey(ctx, cleaned)
	if err != nil {
		return err
	}
	defer s.releaseKey(cleaned, ch)

	if err := s.mu.Lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()

	exists, err := s.ObjectStore.Exists(ctx, cleaned)
	if err != nil {
		return err
	}
	jPath := casJournalPath(cleaned)
	if !exists {
		journalExists, err := s.ObjectStore.Exists(ctx, jPath)
		if err != nil {
			return err
		}
		if !journalExists {
			s.setJournaled(cleaned, false)
			return s.ObjectStore.Delete(ctx, cleaned)
		}
	}

	err = s.ObjectStore.Delete(ctx, jPath)
	journalGone := err == nil || errors.Is(err, ErrNotFound)
	if journalGone {
		s.setJournaled(cleaned, false)
	}
	if !exists || !journalGone {
		return err
	}
	return s.ObjectStore.Delete(ctx, cleaned)
}

func (s *singleProcessStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	raw, err := s.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	present := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	var journals []string
	for _, k := range raw {
		if target, ok := casTargetPath(k); ok {
			journals = append(journals, target)
			continue
		}
		present[k] = struct{}{}
		out = append(out, k)
	}
	for _, target := range journals {
		if _, ok := present[target]; !ok && strings.HasPrefix(target, prefix) {
			present[target] = struct{}{}
			out = append(out, target)
		}
	}
	return out, nil
}

type singleProcessRangeStore struct {
	*singleProcessStore
	ranges RangeReader
}

func (s singleProcessRangeStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	if isCASJournal(path) {
		return nil, ErrInvalidPath
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.ranges.ReadRange(ctx, path, offset, length)
	if !errors.Is(err, ErrNotFound) {
		return data, err
	}
	jdata, jerr := s.ranges.ReadRange(ctx, casJournalPath(path), offset, length)
	if errors.Is(jerr, ErrNotFound) {
		return nil, err
	}
	return jdata, jerr
}

func (s singleProcessRangeStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	if isCASJournal(path) {
		return nil, ErrInvalidPath
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ra, fromJournal, err := s.openReaderAtLocked(ctx, path)
	if err != nil {
		return nil, err
	}
	c, closable := ra.(io.Closer)
	if fromJournal {
		// A journal handle can keep serving stale bytes after replacement.
		// Resolve the logical key on every read instead of retaining it.
		if closable {
			closer(c)()
		}
		ra, c = nil, nil
	}
	wrapped := &singleProcessReaderAt{
		store: s,
		ctx:   ctx,
		path:  path,
		ra:    ra,
	}
	if closable {
		return &singleProcessReadAtCloser{singleProcessReaderAt: wrapped, closer: c}, nil
	}
	return wrapped, nil
}

func (s singleProcessRangeStore) openReaderAtLocked(ctx context.Context, path string) (ra io.ReaderAt, fromJournal bool, err error) {
	ra, err = s.ranges.ReaderAt(ctx, path)
	if !errors.Is(err, ErrNotFound) {
		return ra, false, err
	}
	jra, jerr := s.ranges.ReaderAt(ctx, casJournalPath(path))
	if errors.Is(jerr, ErrNotFound) {
		return nil, false, err
	}
	return jra, true, jerr
}

type singleProcessReaderAt struct {
	store singleProcessRangeStore
	ctx   context.Context
	path  string
	ra    io.ReaderAt
}

func (r *singleProcessReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	if r.ra != nil {
		n, err := r.ra.ReadAt(p, off)
		if !errors.Is(err, ErrNotFound) {
			return n, err
		}
	}
	ra, _, err := r.store.openReaderAtLocked(r.ctx, r.path)
	if err != nil {
		return 0, err
	}
	if c, ok := ra.(io.Closer); ok {
		defer closer(c)()
	}
	return ra.ReadAt(p, off)
}

type singleProcessReadAtCloser struct {
	*singleProcessReaderAt
	closer io.Closer
	closed atomic.Bool
}

func (r *singleProcessReadAtCloser) ReadAt(p []byte, off int64) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return r.singleProcessReaderAt.ReadAt(p, off)
}

func (r *singleProcessReadAtCloser) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	r.closed.Store(true)
	return nil
}

type singleProcessPrefixStore struct {
	*singleProcessStore
	prefixes PrefixLister
}

func (s singleProcessPrefixStore) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prefixes.ListPrefixes(ctx, prefix)
}

type singleProcessFullStore struct {
	singleProcessRangeStore
	prefixes PrefixLister
}

func (s singleProcessFullStore) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prefixes.ListPrefixes(ctx, prefix)
}

func (s *singleProcessStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	cleaned, valid := storepath.Key(path)
	if !valid || isCASJournal(cleaned) {
		return ErrInvalidPath
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ch, err := s.acquireKey(ctx, cleaned)
	if err != nil {
		return err
	}
	defer s.releaseKey(cleaned, ch)

	if err := s.mu.Lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()

	current, exists, fromJournal, err := s.readStateLocked(ctx, cleaned)
	if err != nil {
		return err
	}
	if (exists && current != expected) || (!exists && expected != "") {
		return ErrSnapshotConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !exists {
		if err := s.ObjectStore.Put(ctx, cleaned, strings.NewReader(replacement)); err != nil {
			if errors.Is(err, ErrPathExists) {
				return ErrSnapshotConflict
			}
			return fmt.Errorf("single-process cas put: %w", err)
		}
		return nil
	}
	if !fromJournal && expected == replacement {
		return nil
	}

	jPath := casJournalPath(cleaned)
	if !fromJournal {
		if err := s.stageJournalLocked(ctx, jPath, expected); err != nil {
			return err
		}
		s.setJournaled(cleaned, true)
		if err := s.ObjectStore.Delete(ctx, cleaned); err != nil {
			return fmt.Errorf("single-process cas delete: %w", err)
		}
	}

	if err := ctx.Err(); err != nil {
		s.restoreAfterFailedPutLocked(ctx, cleaned, jPath, expected)
		return err
	}
	if err := s.ObjectStore.Put(ctx, cleaned, strings.NewReader(replacement)); err != nil {
		s.restoreAfterFailedPutLocked(ctx, cleaned, jPath, expected)
		return fmt.Errorf("single-process cas put: %w", err)
	}

	s.clearJournalLocked(ctx, cleaned, jPath)
	return nil
}

func (s *singleProcessStore) stageJournalLocked(ctx context.Context, jPath, expected string) error {
	err := s.ObjectStore.Put(ctx, jPath, strings.NewReader(expected))
	if errors.Is(err, ErrPathExists) {
		if delErr := s.ObjectStore.Delete(ctx, jPath); delErr != nil && !errors.Is(delErr, ErrNotFound) {
			return fmt.Errorf("single-process cas journal cleanup: %w", delErr)
		}
		err = s.ObjectStore.Put(ctx, jPath, strings.NewReader(expected))
	}
	if err != nil {
		return fmt.Errorf("single-process cas journal: %w", err)
	}
	return nil
}

// singleProcessCleanupTimeout bounds each step after a swap that ignores the
// caller's cancellation, because the store's lock is held for it. A step that
// does not finish leaves the journal, which serves the prior content and which
// the next swap resolves.
const singleProcessCleanupTimeout = 10 * time.Second

func (s *singleProcessStore) restoreAfterFailedPutLocked(ctx context.Context, cleaned, jPath, expected string) {
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleProcessCleanupTimeout)
	defer cancel()
	restoreErr := s.ObjectStore.Put(restoreCtx, cleaned, strings.NewReader(expected))
	if restoreErr == nil || errors.Is(restoreErr, ErrPathExists) {
		s.clearJournalLocked(ctx, cleaned, jPath)
	}
}

func (s *singleProcessStore) clearJournalLocked(ctx context.Context, cleaned, jPath string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleProcessCleanupTimeout)
	defer cancel()
	delErr := s.ObjectStore.Delete(ctx, jPath)
	if delErr == nil || errors.Is(delErr, ErrNotFound) {
		s.setJournaled(cleaned, false)
	}
}

func (s *singleProcessStore) readStateLocked(ctx context.Context, path string) (content string, exists, fromJournal bool, err error) {
	content, exists, err = s.readRawLocked(ctx, path)
	if err != nil || exists {
		return content, exists, false, err
	}
	content, exists, err = s.readRawLocked(ctx, casJournalPath(path))
	if err != nil || !exists {
		return "", false, false, err
	}
	s.setJournaled(path, true)
	return content, true, true, nil
}

func (s *singleProcessStore) readRawLocked(ctx context.Context, path string) (string, bool, error) {
	rc, err := s.ObjectStore.Get(ctx, path)
	if errors.Is(err, ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("single-process cas read: %w", err)
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", false, fmt.Errorf("lode: single-process cas read: %w", err)
	}
	return string(data), true, nil
}
