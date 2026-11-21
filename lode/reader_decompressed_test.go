package lode_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"
)

func TestOpenDecompressed_StreamsAndVerifiesChecksums(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		comp lode.Compressor
	}{
		{"uncompressed", lode.Uncompressed()},
		{"gzip", lode.Gzip()},
		{"zstd", lode.Zstd()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := lode.NewMemory()
			root := lode.Open(lode.Use(store))

			blobs, err := root.Blobs(ctx, "docs", lode.WithCompressor(tc.comp), lode.WithChecksum(lode.SHA256()))
			if err != nil {
				t.Fatal(err)
			}
			want := []byte(strings.Repeat("streaming-decompressed-payload-", 64))
			snap, err := blobs.Write(ctx, want, nil)
			if err != nil {
				t.Fatal(err)
			}

			view, err := lode.OpenReadOnly(lode.Use(store)).Blobs(ctx, "docs")
			if err != nil {
				t.Fatal(err)
			}
			rc, err := view.OpenDecompressed(ctx, snap.Manifest, snap.Objects[0])
			if err != nil {
				t.Fatalf("OpenDecompressed = %v", err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll(OpenDecompressed) = %v", err)
			}
			if err := rc.Close(); err != nil {
				t.Fatalf("Close = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("payload len = %d, want %d", len(got), len(want))
			}

			// The manifest owns the digest. A detached caller reference can carry stale metadata.
			ref := snap.Objects[0]
			ref.Checksum = ""
			rc, err = view.OpenDecompressed(ctx, snap.Manifest, ref)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(io.Discard, rc)
			if closeErr := rc.Close(); err != nil || closeErr != nil {
				t.Fatalf("reading with manifest digest: read %v, close %v", err, closeErr)
			}

			if err := store.Delete(ctx, ref.Path); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, ref.Path, strings.NewReader("corrupt object")); err != nil {
				t.Fatal(err)
			}
			bad, err := view.OpenDecompressed(ctx, snap.Manifest, ref)
			if err == nil {
				_, err = io.Copy(io.Discard, bad)
				if closeErr := bad.Close(); closeErr != nil {
					t.Errorf("Close: %v", closeErr)
				}
			}
			if !errors.Is(err, lode.ErrChecksumMismatch) {
				t.Fatalf("corrupt object = %v, want ErrChecksumMismatch", err)
			}
		})
	}
}

func TestOpenDecompressed_RejectsForeignDatasetAndSnapshotMismatch(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	root := lode.Open(lode.Use(store))

	a, err := root.Records(ctx, "a", lode.JSONL[lode.D]())
	if err != nil {
		t.Fatal(err)
	}
	snapA1, err := a.Write(ctx, lode.R(lode.D{"n": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	snapA2, err := a.Write(ctx, lode.R(lode.D{"n": 2}), nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := root.Records(ctx, "b", lode.JSONL[lode.D]())
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := b.Write(ctx, lode.R(lode.D{"n": 3}), nil)
	if err != nil {
		t.Fatal(err)
	}

	unlisted := snapA1.Objects[0]
	unlisted.Path += ".orphan"
	if _, err := a.OpenDecompressed(ctx, snapA1.Manifest, unlisted); !errors.Is(err, lode.ErrInvalidArgument) {
		t.Fatalf("unlisted object = %v, want ErrInvalidArgument", err)
	}
	if _, err := a.OpenDecompressed(ctx, snapB.Manifest, snapB.Objects[0]); !errors.Is(err, lode.ErrManifestInvalid) {
		t.Fatalf("foreign manifest = %v, want ErrManifestInvalid", err)
	}
	if _, err := a.OpenDecompressed(ctx, snapA1.Manifest, snapA2.Objects[0]); !errors.Is(err, lode.ErrInvalidArgument) {
		t.Fatalf("cross-snapshot object = %v, want ErrInvalidArgument", err)
	}
}
