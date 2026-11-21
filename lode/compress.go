package lode

import (
	"compress/gzip"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// -----------------------------------------------------------------------------
// Gzip Compressor
// -----------------------------------------------------------------------------

// gzipCompressor implements Compressor using gzip compression.
type gzipCompressor struct{}

// Gzip returns a gzip compressor (.gz, wire name "gzip").
func Gzip() Compressor {
	return &gzipCompressor{}
}

func (g *gzipCompressor) Name() string      { return "gzip" }
func (g *gzipCompressor) Extension() string { return ".gz" }

func (g *gzipCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (g *gzipCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

// -----------------------------------------------------------------------------
// Zstd Compressor
// -----------------------------------------------------------------------------

// zstdCompressor implements Compressor using Zstandard compression. It reuses
// its encoders: a new encoder allocates its whole state, about 2 MiB, which a
// dataset that commits one small object at a time would otherwise pay on
// every write. Each encoder runs on one goroutine, which also compresses a
// large stream faster than the default concurrency (docs/BENCHMARKS.md).
type zstdCompressor struct {
	encoders sync.Pool
}

// Zstd returns a Zstandard compressor (.zst, wire name "zstd").
func Zstd() Compressor {
	return &zstdCompressor{}
}

func (z *zstdCompressor) Name() string      { return "zstd" }
func (z *zstdCompressor) Extension() string { return ".zst" }

func (z *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	enc, ok := z.encoders.Get().(*zstd.Encoder)
	if !ok {
		var err error
		if enc, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1)); err != nil {
			return nil, err
		}
	}
	enc.Reset(w)
	return &zstdWriter{enc: enc, pool: &z.encoders}, nil
}

// errZstdWriterClosed is returned by a write to a zstdWriter after Close,
// whose encoder may already compress another stream.
var errZstdWriterClosed = errors.New("write to a closed zstd writer")

// zstdWriter is one stream of a reused encoder. Close finishes the stream
// and returns the encoder for the next stream, unless finishing failed.
type zstdWriter struct {
	enc  *zstd.Encoder
	pool *sync.Pool
}

func (w *zstdWriter) Write(p []byte) (int, error) {
	if w.enc == nil {
		return 0, errZstdWriterClosed
	}
	return w.enc.Write(p)
}

func (w *zstdWriter) Close() error {
	if w.enc == nil {
		return nil
	}
	enc := w.enc
	pool := w.pool
	w.enc = nil
	w.pool = nil
	if err := enc.Close(); err != nil {
		return err
	}
	// Close leaves the output writer attached to the encoder.
	enc.Reset(nil)
	pool.Put(enc)
	return nil
}

func (z *zstdCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return decoder.IOReadCloser(), nil
}

// -----------------------------------------------------------------------------
// Uncompressed (No-Op) Compressor
// -----------------------------------------------------------------------------

// noopCompressor implements Compressor with no compression.
type noopCompressor struct{}

// Uncompressed returns a pass-through compressor with no file extension
// (wire name "noop", the default when WithCompressor is omitted).
func Uncompressed() Compressor {
	return &noopCompressor{}
}

func (n *noopCompressor) Name() string      { return "noop" }
func (n *noopCompressor) Extension() string { return "" }

func (n *noopCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &noopWriteCloser{w}, nil
}

func (n *noopCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

type noopWriteCloser struct {
	io.Writer
}

func (n *noopWriteCloser) Close() error { return nil }
