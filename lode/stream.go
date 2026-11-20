package lode

import (
	"context"
	"fmt"
	"io"
	"iter"
	"sync"
)

// -----------------------------------------------------------------------------
// Streamed staging (Blobs.StreamWrite, Records.StreamWrite)
// -----------------------------------------------------------------------------

// streamBlob streams raw bytes from r into the snapshot's blob object. When
// uncompressed, r passes directly to store.Put without an io.Pipe or background
// goroutine; when compressed, bytes pass through dataStream.
func (d *writer) streamBlob(ctx context.Context, id SnapshotID, r io.Reader) (ObjectRef, error) {
	if _, uncompressed := d.components.compressor.(*noopCompressor); uncompressed {
		path := d.filePath(id, "", blobFile)
		cr := &countingReader{r: r}
		src := io.Reader(cr)
		var hasher HashWriter
		if d.components.checksum != nil {
			hasher = d.components.checksum.NewHasher()
			src = io.TeeReader(cr, hasher)
		}
		if err := d.store.Put(ctx, path, src); err != nil {
			return ObjectRef{}, fmt.Errorf("failed to write data: %w", err)
		}
		ref := ObjectRef{Path: path, SizeBytes: cr.n}
		if hasher != nil {
			ref.Checksum = hasher.Sum()
		}
		return ref, nil
	}

	stream, err := d.openStream(ctx, id, blobFile)
	if err != nil {
		return ObjectRef{}, err
	}
	if _, err := io.Copy(stream, r); err != nil {
		_, cause := stream.put.rootCause(err)
		stream.abort(cause)
		return ObjectRef{}, fmt.Errorf("failed to write data: %w", cause)
	}
	return stream.finish()
}

type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

type dataStream struct {
	path      string
	reader    *io.PipeReader
	pipe      *io.PipeWriter
	comp      io.WriteCloser
	count     countingWriter
	hasher    HashWriter
	put       putResult
	cancelPut context.CancelFunc
}

func (d *writer) openStream(ctx context.Context, id SnapshotID, role string) (*dataStream, error) {
	pr, pw := io.Pipe()
	path := d.filePath(id, "", role)
	s := &dataStream{
		path:   path,
		reader: pr,
		pipe:   pw,
		count:  countingWriter{w: pw},
	}

	var sink io.Writer = &s.count
	if d.components.checksum != nil {
		s.hasher = d.components.checksum.NewHasher()
		sink = io.MultiWriter(&s.count, s.hasher)
	}

	putCtx, cancel := context.WithCancel(ctx)
	s.cancelPut = cancel
	s.put.start(putCtx, ctx, d.store, path, pr)
	comp, err := d.components.compressor.Compress(sink)
	if err != nil {
		fromPut, cause := s.put.rootCause(err)
		s.abort(cause)
		if fromPut {
			return nil, fmt.Errorf("failed to write data: %w", cause)
		}
		return nil, fmt.Errorf("failed to create compressor: %w", cause)
	}
	s.comp = comp
	return s, nil
}

func (s *dataStream) Write(p []byte) (int, error) {
	if err := s.failed(); err != nil {
		return 0, err
	}
	return s.comp.Write(p)
}

func (s *dataStream) failed() error {
	if finished, err := s.put.poll(); finished {
		return err
	}
	return nil
}

func (s *dataStream) finish() (ObjectRef, error) {
	if err := s.comp.Close(); err != nil {
		return ObjectRef{}, s.fail("compressor", err)
	}
	if err := s.pipe.Close(); err != nil {
		return ObjectRef{}, s.fail("pipe", err)
	}
	err := s.put.wait()
	s.cancelPut()
	if err != nil {
		return ObjectRef{}, fmt.Errorf("failed to write data: %w", err)
	}

	ref := ObjectRef{Path: s.path, SizeBytes: s.count.n}
	if s.hasher != nil {
		ref.Checksum = s.hasher.Sum()
	}
	return ref, nil
}

func (s *dataStream) fail(step string, err error, upstream ...io.Closer) error {
	fromPut, cause := s.put.rootCause(err)
	s.cancelUpload(cause)
	for _, c := range upstream {
		_ = c.Close()
	}
	_ = s.put.wait()
	if fromPut {
		return fmt.Errorf("failed to write data: %w", cause)
	}
	return fmt.Errorf("failed to close %s: %w", step, cause)
}

func (s *dataStream) cancelUpload(cause error) {
	if cause == nil {
		cause = io.ErrClosedPipe
	}
	_ = s.reader.CloseWithError(cause)
	_ = s.pipe.CloseWithError(cause)
	s.cancelPut()
}

func (s *dataStream) abort(cause error) {
	s.cancelUpload(cause)
	if s.comp != nil {
		_ = s.comp.Close()
	}
	_ = s.put.wait()
}

type recordStream[T any] struct {
	*dataStream
	encoder     StreamEncoder[T]
	timestamped bool
	rows        int64
	timestamps  timestampRange
}

func openRecordStream[T any](ctx context.Context, d *writer, id SnapshotID, codec StreamingCodec[T], timestamped bool) (*recordStream[T], error) {
	s, err := d.openStream(ctx, id, dataFile)
	if err != nil {
		return nil, err
	}
	encoder, err := codec.NewStreamEncoder(s.comp)
	if err != nil {
		fromPut, cause := s.put.rootCause(err)
		s.abort(cause)
		if fromPut {
			return nil, fmt.Errorf("failed to write data: %w", cause)
		}
		return nil, fmt.Errorf("failed to create stream encoder: %w", cause)
	}
	return &recordStream[T]{dataStream: s, encoder: encoder, timestamped: timestamped}, nil
}

func (r *recordStream[T]) encode(records iter.Seq2[T, error]) error {
	if err := r.putFailed(); err != nil {
		return err
	}
	for record, iterErr := range records {
		if err := r.putFailed(); err != nil {
			return err
		}
		if iterErr != nil {
			r.abort(iterErr)
			return fmt.Errorf("record iterator error: %w", iterErr)
		}
		if err := r.write(record); err != nil { //nolint:errjoin // reason: each record is written only after the upload is known to be live
			return err
		}
		if err := r.putFailed(); err != nil { //nolint:errjoin // reason: an upload failure stops the iteration before the next record
			return err
		}
	}
	return r.putFailed()
}

func (r *recordStream[T]) write(record T) error {
	if err := r.encoder.WriteRecord(record); err != nil {
		r.abort(err)
		return fmt.Errorf("failed to write record: %w", err)
	}
	r.rows++
	if r.timestamped {
		r.timestamps.observe(record)
	}
	return nil
}

func (r *recordStream[T]) putFailed() error {
	err := r.failed()
	if err == nil {
		return nil
	}
	r.abort(err)
	return fmt.Errorf("failed to write data: %w", err)
}

func (r *recordStream[T]) finish() (ObjectRef, error) {
	if err := r.encoder.Close(); err != nil {
		return ObjectRef{}, r.fail("encoder", err, r.comp)
	}
	ref, err := r.dataStream.finish()
	if err != nil {
		return ObjectRef{}, err
	}
	ref.Stats = streamEncoderStats(r.encoder)
	return ref, nil
}

func (r *recordStream[T]) abort(cause error) {
	r.cancelUpload(cause)
	_ = r.encoder.Close()
	if r.comp != nil {
		_ = r.comp.Close()
	}
	_ = r.put.wait()
}

type putResult struct {
	done     chan struct{}
	mu       sync.Mutex
	finished bool
	err      error
}

func (r *putResult) start(putCtx, callerCtx context.Context, store Store, path string, pr *io.PipeReader) {
	r.done = make(chan struct{})
	var stopCancel func() bool
	// Local aborts close the pipe themselves. Watch only caller cancellation.
	if callerCtx.Done() != nil {
		stopCancel = context.AfterFunc(callerCtx, func() {
			_ = pr.CloseWithError(callerCtx.Err())
		})
	}
	go func() {
		err := store.Put(putCtx, path, pr)
		if stopCancel != nil {
			stopCancel()
		}
		r.mu.Lock()
		r.err, r.finished = err, true
		r.mu.Unlock()
		_ = pr.CloseWithError(err)
		close(r.done)
	}()
}

func (r *putResult) wait() error {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *putResult) poll() (finished bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished, r.err
}

func (r *putResult) rootCause(stepErr error) (fromPut bool, cause error) {
	if finished, putErr := r.poll(); finished && putErr != nil {
		return true, putErr
	}
	return false, stepErr
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}
