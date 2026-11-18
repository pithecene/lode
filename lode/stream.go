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
	path   string
	reader *io.PipeReader
	pipe   *io.PipeWriter
	comp   io.WriteCloser
	count  *countingWriter
	hasher HashWriter
	put    *putResult
}

func (d *writer) newStream(id SnapshotID, role string) (*dataStream, error) {
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
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}
	return &dataStream{
		path:   d.filePath(id, "", role),
		reader: pr,
		pipe:   pw,
		comp:   comp,
		count:  count,
		hasher: hasher,
	}, nil
}

func (d *writer) openStream(ctx context.Context, id SnapshotID, role string) (*dataStream, error) {
	s, err := d.newStream(id, role)
	if err != nil {
		return nil, err
	}
	s.start(ctx, d.store)
	return s, nil
}

func (s *dataStream) start(ctx context.Context, store Store) {
	s.put = startPut(ctx, store, s.path, s.reader)
}

func (s *dataStream) Write(p []byte) (int, error) {
	if err := s.failed(); err != nil {
		return 0, err
	}
	return s.comp.Write(p)
}

func (s *dataStream) failed() error {
	if s.put == nil {
		return nil
	}
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
	if err := s.put.wait(); err != nil {
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
	for _, c := range upstream {
		_ = c.Close()
	}
	s.abort(cause)
	if fromPut {
		return fmt.Errorf("failed to write data: %w", cause)
	}
	return fmt.Errorf("failed to close %s: %w", step, cause)
}

func (s *dataStream) abort(cause error) {
	_ = s.pipe.CloseWithError(cause)
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
	s, err := d.newStream(id, dataFile)
	if err != nil {
		return nil, err
	}
	encoder, err := codec.NewStreamEncoder(s.comp)
	if err != nil {
		_ = s.comp.Close()
		_ = s.pipe.Close()
		return nil, fmt.Errorf("failed to create stream encoder: %w", err)
	}
	s.start(ctx, d.store)
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
		if err := r.write(record); err != nil {
			return err
		}
		if err := r.putFailed(); err != nil {
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
	_ = r.encoder.Close()
	_ = r.comp.Close()
	r.dataStream.abort(cause)
}

type putResult struct {
	done     chan struct{}
	mu       sync.Mutex
	finished bool
	err      error
}

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
