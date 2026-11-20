package lode

import (
	"compress/gzip"
	"io"

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

// zstdCompressor implements Compressor using Zstandard compression.
type zstdCompressor struct{}

// Zstd returns a Zstandard compressor (.zst, wire name "zstd").
func Zstd() Compressor {
	return &zstdCompressor{}
}

func (z *zstdCompressor) Name() string      { return "zstd" }
func (z *zstdCompressor) Extension() string { return ".zst" }

func (z *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w)
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
