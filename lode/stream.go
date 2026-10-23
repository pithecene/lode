package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
)

// -----------------------------------------------------------------------------
// Streamed staging (Blobs.StreamWrite, Records.StreamWrite)
// -----------------------------------------------------------------------------

// dataStream streams one data file: bytes pass through the compressor, are
// counted and checksummed, and flow down a pipe into a background store.Put.
// Writes fail fast once the Put has failed, since a buffering compressor may
// otherwise absorb them without touching the pipe.
type dataStream struct {
	path   string
	reader *io.PipeReader // drained by the Put
	pipe   *io.PipeWriter
	comp   io.WriteCloser
	count  *countingWriter
	hasher HashWriter
	put    *putResult // nil until start
}

// newStream builds the pipeline for a data file of the given role without
// starting its Put, so a caller can wrap the compressed side first.
func (d *writer) newStream(id DatasetSnapshotID, role string) (*dataStream, error) {
	pr, pw := io.Pipe()
	count := &countingWriter{w: pw}

	var sink io.Writer = count
	var hasher HashWriter
	if d.components.checksum != nil {
		hasher = d.components.checksum.NewHasher()
		sink = io.MultiWriter(count, hasher)
	}

	comp, err := d.components.compressor.Compress(sink)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create compressor: %w", err)
	}
	return &dataStream{
		path:   d.filePath(id, "", role),
		reader: pr,
		pipe:   pw,
		comp:   comp,
		count:  count,
		hasher: hasher,
		put:    nil,
	}, nil
}

// openStream builds a data stream and starts its Put.
func (d *writer) openStream(ctx context.Context, id DatasetSnapshotID, role string) (*dataStream, error) {
	s, err := d.newStream(id, role)
	if err != nil {
		return nil, err
	}
	s.start(ctx, d.store)
	return s, nil
}

// start launches the background Put that drains the pipe.
func (s *dataStream) start(ctx context.Context, store Store) {
	s.put = startPut(ctx, store, s.path, s.reader)
}

// Write compresses p onto the stream, or reports a Put that already failed.
func (s *dataStream) Write(p []byte) (int, error) {
	if err := s.failed(); err != nil {
		return 0, err
	}
	return s.comp.Write(p)
}

// failed returns the Put's error if it has already returned with one.
func (s *dataStream) failed() error {
	if s.put == nil {
		return nil
	}
	if finished, err := s.put.poll(); finished {
		return err
	}
	return nil
}

// finish flushes the stream, waits for the Put, and describes the stored file.
func (s *dataStream) finish() (FileRef, error) {
	if err := s.comp.Close(); err != nil {
		return FileRef{}, s.fail("compressor", err)
	}
	if err := s.pipe.Close(); err != nil {
		return FileRef{}, s.fail("pipe", err)
	}
	// A failed Put leaves nothing behind (CONTRACT_STORAGE §Put), so there is
	// nothing for the writer to clean up.
	if err := s.put.wait(); err != nil {
		return FileRef{}, fmt.Errorf("lode: failed to write data: %w", err)
	}

	ref := FileRef{Path: s.path, SizeBytes: s.count.n}
	if s.hasher != nil {
		ref.Checksum = s.hasher.Sum()
	}
	return ref, nil
}

// fail reports a failed finalization step after tearing the stream down,
// closing the given upstream writers first. A Put that already failed is the
// root cause of anything that broke after it and is the error reported.
func (s *dataStream) fail(step string, err error, upstream ...io.Closer) error {
	fromPut, cause := s.put.rootCause(err)
	for _, c := range upstream {
		_ = c.Close()
	}
	s.abort(cause)
	if fromPut {
		return fmt.Errorf("lode: failed to write data: %w", cause)
	}
	return fmt.Errorf("lode: failed to close %s: %w", step, cause)
}

// abort fails the pipe with cause and waits for the Put to return. It never
// deletes the data object: a failed Put leaves the path as it was
// (CONTRACT_STORAGE §Put), and a Put that returned ErrPathExists refused a
// path that belongs to another writer.
func (s *dataStream) abort(cause error) {
	_ = s.pipe.CloseWithError(cause)
	_ = s.put.wait()
}

// recordStream encodes records onto a data stream, counting rows and
// observing timestamps as they pass.
type recordStream[T any] struct {
	*dataStream
	encoder     StreamEncoder[T]
	timestamped bool
	rows        int64
	timestamps  timestampRange
}

func openRecordStream[T any](ctx context.Context, d *writer, id DatasetSnapshotID, codec StreamingCodec[T], timestamped bool) (*recordStream[T], error) {
	s, err := d.newStream(id, dataFile)
	if err != nil {
		return nil, err
	}
	encoder, err := codec.NewStreamEncoder(s.comp)
	if err != nil {
		_ = s.comp.Close()
		_ = s.pipe.Close()
		return nil, fmt.Errorf("lode: failed to create stream encoder: %w", err)
	}
	s.start(ctx, d.store)
	return &recordStream[T]{dataStream: s, encoder: encoder, timestamped: timestamped}, nil
}

// encode drains the iterator onto the stream. The Put is checked before the
// first record, after the iterator produces each result, which may block,
// and after each record is written: a buffering compressor can absorb
// records without touching the pipe, so a dead Put would otherwise go
// unnoticed until the iterator ends — which it may never do. Returning from
// the loop stops the iterator.
func (r *recordStream[T]) encode(records iter.Seq2[T, error]) error {
	if err := r.putFailed(); err != nil {
		return err
	}
	for record, iterErr := range records {
		// A Put that failed while the iterator was producing is the root
		// cause and takes precedence over whatever the iterator reports.
		if err := r.putFailed(); err != nil {
			return err
		}
		if iterErr != nil {
			r.abort(iterErr)
			return fmt.Errorf("lode: record iterator error: %w", iterErr)
		}
		if err := r.write(record); err != nil {
			return err
		}
		if err := r.putFailed(); err != nil {
			return err
		}
	}
	// A Put that failed while the iterator was finishing is reported too.
	return r.putFailed()
}

func (r *recordStream[T]) write(record T) error {
	if err := r.encoder.WriteRecord(record); err != nil {
		r.abort(err)
		return fmt.Errorf("lode: failed to write record: %w", err)
	}
	r.rows++
	if r.timestamped {
		r.timestamps.observe(record)
	}
	return nil
}

// putFailed tears the stream down and reports the Put's failure, if any.
func (r *recordStream[T]) putFailed() error {
	err := r.failed()
	if err == nil {
		return nil
	}
	r.abort(err)
	return fmt.Errorf("lode: failed to write data: %w", err)
}

// finish closes the encoder (finalizing the format), then the stream. A
// failing encoder close tears down through the data stream's fail, which
// closes the compressor and the pipe; the encoder is not closed twice.
func (r *recordStream[T]) finish() (FileRef, error) {
	if err := r.encoder.Close(); err != nil {
		return FileRef{}, r.fail("encoder", err, r.comp)
	}
	ref, err := r.dataStream.finish()
	if err != nil {
		return FileRef{}, err
	}
	ref.Stats = fileStats(r.encoder)
	return ref, nil
}

// abort closes the encoder and compressor before failing the pipe. Only
// encode calls it, so the encoder is always open here.
func (r *recordStream[T]) abort(cause error) {
	_ = r.encoder.Close()
	_ = r.comp.Close()
	r.dataStream.abort(cause)
}

// putResult tracks the background store.Put of a streamed data object.
type putResult struct {
	done chan struct{} // closed once the result below is published

	mu       sync.Mutex
	finished bool
	err      error
}

// startPut runs store.Put in the background. The result is published before
// the read end is closed with Put's error, so a writer blocked on the pipe
// fails instead of hanging (a fast failure such as ErrPathExists never drains
// the pipe) and a Write issued after Put returned always observes the result.
func startPut(ctx context.Context, store Store, path string, pr *io.PipeReader) *putResult {
	r := &putResult{done: make(chan struct{})}
	go func() {
		err := store.Put(ctx, path, pr)
		r.mu.Lock()
		r.err, r.finished = err, true
		r.mu.Unlock()
		_ = pr.CloseWithError(err)
		close(r.done)
	}()
	return r
}

// wait blocks until Put has returned and returns its error.
func (r *putResult) wait() error {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// poll reports Put's result if it has already returned, without blocking.
func (r *putResult) poll() (finished bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished, r.err
}

// rootCause picks the error to surface when a finalization step fails: a Put
// that has already failed is the root cause of anything that broke after it.
func (r *putResult) rootCause(stepErr error) (fromPut bool, cause error) {
	if finished, putErr := r.poll(); finished && putErr != nil {
		return true, putErr
	}
	return false, stepErr
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// -----------------------------------------------------------------------------
// StreamWriter
// -----------------------------------------------------------------------------

// streamWriter implements StreamWriter for raw binary streaming writes.
type streamWriter struct {
	ds      *writer
	ctx     context.Context
	pending pendingSnapshot
	stream  *dataStream

	mu        sync.Mutex
	committed bool
	aborted   bool
	closed    bool
	writeErr  error
}

func (sw *streamWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	if sw.closed || sw.aborted || sw.committed {
		sw.mu.Unlock()
		return 0, errors.New("lode: stream is closed")
	}
	if sw.writeErr != nil {
		sw.mu.Unlock()
		return 0, sw.writeErr
	}
	sw.mu.Unlock()

	n, err = sw.stream.Write(p)
	if err != nil {
		sw.mu.Lock()
		sw.writeErr = err
		sw.mu.Unlock()
	}
	return n, err
}

func (sw *streamWriter) Commit(ctx context.Context) (*DatasetSnapshot, error) {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream already committed")
	}
	if sw.aborted || sw.closed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream is closed")
	}
	// Mark as closed to prevent concurrent commits, but not committed until success
	sw.closed = true
	sw.mu.Unlock()

	ref, err := sw.stream.finish()
	if err != nil {
		return nil, err
	}
	sw.pending.add(ref, "")
	sw.pending.rowCount = 1
	sw.pending.streamed = ref.Path

	// The data file is persisted; on conflict the commit only re-parents the
	// manifest and retries the pointer CAS.
	snapshot, err := sw.ds.commit(ctx, &sw.pending)
	if err != nil {
		return nil, err
	}

	sw.mu.Lock()
	sw.committed = true
	sw.mu.Unlock()

	return snapshot, nil
}

func (sw *streamWriter) Abort(_ context.Context) error {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return errors.New("lode: cannot abort committed stream")
	}
	if sw.aborted {
		sw.mu.Unlock()
		return nil // already aborted
	}
	sw.aborted = true
	sw.mu.Unlock()

	sw.stream.abort(errors.New("stream aborted"))
	return nil
}

func (sw *streamWriter) Close() error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return nil
	}
	sw.closed = true
	alreadyCommitted := sw.committed
	alreadyAborted := sw.aborted
	sw.mu.Unlock()

	if alreadyCommitted || alreadyAborted {
		return nil
	}

	// Close without commit = abort
	return sw.Abort(sw.ctx)
}
