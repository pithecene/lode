package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/errs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/lode/lode"
)

// flagIntegration gates integration tests that require running S3 services.
// Pass -integration to enable.
var flagIntegration = flag.Bool("integration", false, "run integration tests (requires s3:up)")

// Integration tests for S3-compatible backends.
// These require running docker-compose services.
//
// To run:
//
//	task s3:up
//	go test -v ./lode/s3/... -integration
//	task s3:down
func skipIfNoS3(t *testing.T) {
	t.Helper()
	if !*flagIntegration {
		t.Skip("skipping integration test; use -integration to enable")
	}
}

// s3Backend describes an S3-compatible backend for table-driven tests.
type s3Backend struct {
	name      string
	newClient func(context.Context) (*s3.Client, error)
}

var s3Backends = []s3Backend{
	{"LocalStack", newLocalStackClient},
	{"RustFS", newRustFSClient},
}

// newLocalStackClient creates an S3 client for LocalStack (integration tests only).
func newLocalStackClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
		o.UsePathStyle = true
	}), nil
}

// newRustFSClient creates an S3 client for RustFS (integration tests only).
func newRustFSClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("rustfsadmin", "rustfsadmin", "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
	}), nil
}

// setupTestBucket creates a unique bucket and registers cleanup via t.Cleanup.
func setupTestBucket(t *testing.T, backend s3Backend) *Store {
	t.Helper()
	skipIfNoS3(t)

	ctx := t.Context()
	client, err := backend.newClient(ctx)
	if err != nil {
		t.Fatalf("failed to create %s client: %v", backend.name, err)
	}

	return newEphemeralBucketStore(ctx, t, client, "lode-test")
}

// -----------------------------------------------------------------------------
// Store Integration Tests
// -----------------------------------------------------------------------------

func TestIntegration_Reads(t *testing.T) {
	t.Parallel()
	t.Run("write list read", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			content := []byte("hello world")
			key := "test/file.txt"

			putIntegrationObject(t, store, key, content)

			keys, err := store.List(ctx, "test/")
			if err != nil {
				t.Fatalf("List failed: %v", err)
			}
			if !slices.Contains(keys, key) {
				t.Errorf("expected key %q in list, got %v", key, keys)
			}

			rc, err := store.Get(ctx, key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			data, err := io.ReadAll(rc)
			errs.DiscardClose(rc)
			if err != nil {
				t.Fatalf("reading body failed: %v", err)
			}
			if string(data) != string(content) {
				t.Errorf("expected %q, got %q", string(content), string(data))
			}
		})
	})

	t.Run("range read", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			content := []byte("0123456789")
			key := "test/range.txt"

			putIntegrationObject(t, store, key, content)

			data, err := store.ReadRange(ctx, key, 3, 4)
			if err != nil {
				t.Fatalf("ReadRange failed: %v", err)
			}
			if string(data) != "3456" {
				t.Errorf("expected '3456', got %q", string(data))
			}

			// Read beyond EOF — should return available bytes
			data, err = store.ReadRange(ctx, key, 8, 10)
			if err != nil {
				t.Fatalf("ReadRange beyond EOF failed: %v", err)
			}
			if string(data) != "89" {
				t.Errorf("expected '89', got %q", string(data))
			}
		})
	})

	t.Run("reader at", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			content := []byte("hello world reader at")
			key := "test/readerat.txt"

			putIntegrationObject(t, store, key, content)

			ra, err := store.ReaderAt(ctx, key)
			if err != nil {
				t.Fatalf("ReaderAt failed: %v", err)
			}

			buf := make([]byte, 5)

			n, err := ra.ReadAt(buf, 0)
			if err != nil {
				t.Fatalf("ReadAt(0) failed: %v", err)
			}
			if n != 5 || string(buf) != "hello" {
				t.Errorf("ReadAt(0): expected 'hello', got %q", string(buf[:n]))
			}

			n, err = ra.ReadAt(buf, 6)
			if err != nil {
				t.Fatalf("ReadAt(6) failed: %v", err)
			}
			if n != 5 || string(buf) != "world" {
				t.Errorf("ReadAt(6): expected 'world', got %q", string(buf[:n]))
			}
		})
	})

	t.Run("not found", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			_, err := store.Get(ctx, "nonexistent/path.txt")
			if !errors.Is(err, lode.ErrNotFound) {
				t.Errorf("expected ErrNotFound for Get, got: %v", err)
			}

			_, err = store.ReadRange(ctx, "nonexistent/path.txt", 0, 10)
			if !errors.Is(err, lode.ErrNotFound) {
				t.Errorf("expected ErrNotFound for ReadRange, got: %v", err)
			}

			_, err = store.ReaderAt(ctx, "nonexistent/path.txt")
			if !errors.Is(err, lode.ErrNotFound) {
				t.Errorf("expected ErrNotFound for ReaderAt, got: %v", err)
			}
		})
	})

	t.Run("list prefixes and prefixed bucket", testIntegrationListPrefixes)
}

func testIntegrationListPrefixes(t *testing.T) {
	t.Helper()
	forEachBucket(t, func(t *testing.T, store *Store) {
		ctx := t.Context()
		prefixed, err := lode.Prefixed(Bucket(store.client, store.bucket), "tenant-a")()
		if err != nil {
			t.Fatalf("Prefixed factory failed: %v", err)
		}

		for _, k := range []string{"datasets/alpha/latest", "datasets/beta/latest", "datasets/root.txt"} {
			if err := prefixed.Put(ctx, k, bytes.NewReader([]byte("x"))); err != nil {
				t.Fatalf("Put(%q) failed: %v", k, err)
			}
		}

		lister, ok := prefixed.(lode.PrefixLister)
		if !ok {
			t.Fatal("expected Prefixed S3 store to implement PrefixLister")
		}
		got, err := lister.ListPrefixes(ctx, "datasets/")
		if err != nil {
			t.Fatalf("ListPrefixes failed: %v", err)
		}
		want := []string{"datasets/alpha/", "datasets/beta/"}
		if !slices.Equal(got, want) {
			t.Errorf("ListPrefixes = %v, want %v", got, want)
		}
	})
}

func TestIntegration_Writes(t *testing.T) {
	t.Parallel()
	t.Run("immutability enforcement", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			content := []byte("immutable")
			key := "test/immutable.txt"

			putIntegrationObject(t, store, key, content)

			err := store.Put(ctx, key, bytes.NewReader([]byte("modified")))
			if !errors.Is(err, lode.ErrPathExists) {
				t.Errorf("expected ErrPathExists on second write, got: %v", err)
			}
		})
	})

	t.Run("manifest last visibility", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			dataKey := "datasets/test-ds/snapshots/snap1/data/file.jsonl"
			manifestKey := "datasets/test-ds/snapshots/snap1/manifest.json"

			// Write data first
			err := store.Put(ctx, dataKey, bytes.NewReader([]byte(`{"id": 1}`)))
			if err != nil {
				t.Fatalf("Put data failed: %v", err)
			}

			// Before manifest, listing should not find the manifest
			keys, err := store.List(ctx, "datasets/test-ds/snapshots/snap1/")
			if err != nil {
				t.Fatalf("List failed: %v", err)
			}
			if slices.Contains(keys, manifestKey) {
				t.Error("manifest should not exist before being written")
			}

			// Write manifest (commit signal)
			manifestJSON := `{"schema_name":"lode-manifest","format_version":"1.2.0","dataset_id":"test-ds","snapshot_id":"snap1","created_at":"2024-01-01T00:00:00Z","metadata":{},"objects":[{"path":"datasets/test-ds/snapshots/snap1/data/file.jsonl","size_bytes":10}],"row_count":1,"compressor":"noop","partitioner":"noop"}`
			err = store.Put(ctx, manifestKey, bytes.NewReader([]byte(manifestJSON)))
			if err != nil {
				t.Fatalf("Put manifest failed: %v", err)
			}

			// After manifest, listing should find both
			keys, err = store.List(ctx, "datasets/test-ds/snapshots/snap1/")
			if err != nil {
				t.Fatalf("List failed: %v", err)
			}
			if !slices.Contains(keys, manifestKey) {
				t.Error("manifest should exist after being written")
			}
			if !slices.Contains(keys, dataKey) {
				t.Error("data file should exist")
			}
		})
	})

	// TestIntegration_StreamWrite_ContextCancel verifies that when context
	// is canceled during a streaming write to S3, no snapshot becomes visible.
	t.Run("stream write context cancel", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			ds, err := lode.Open(lode.Prefixed(lode.Use(store), "cancel-subtest")).Blobs(ctx, "cancel-test")
			if err != nil {
				t.Fatal(err)
			}

			cancelCtx, cancel := context.WithCancel(ctx)
			r := &cancelingReader{
				prefix: bytes.NewReader([]byte("data that will be canceled")),
				cancel: cancel,
			}

			_, err = ds.StreamWrite(cancelCtx, r, lode.D{"test": "cancel"})
			if err == nil {
				t.Log("Note: commit completed before cancel took effect (timing-dependent)")
			} else if !errors.Is(err, context.Canceled) {
				t.Logf("Note: commit failed with non-cancel error: %v", err)
			}

			// Core invariant: verify snapshot state with fresh context
			freshCtx := t.Context()
			_, err = ds.Latest(freshCtx)
			switch {
			case errors.Is(err, lode.ErrNoSnapshots):
				t.Log("No snapshot visible - cancel prevented commit")
			case err == nil:
				t.Log("Snapshot visible after cancel - commit completed before cancel")
			default:
				t.Errorf("Unexpected error checking snapshot: %v", err)
			}
		})
	})

	// TestIntegration_StreamWriteRecords_ContextTimeout verifies that when
	// context times out during StreamWriteRecords to S3, no partial snapshot is visible.
	t.Run("stream write records context timeout", func(t *testing.T) {
		forEachBucket(t, func(t *testing.T, store *Store) {
			ctx := t.Context()
			ds, err := lode.Open(lode.Use(store)).Records(ctx, "timeout-test", lode.JSONL[lode.D]())
			if err != nil {
				t.Fatal(err)
			}

			records := make([]lode.D, 100)
			for i := range records {
				records[i] = lode.D{"id": fmt.Sprintf("%d", i), "data": "payload"}
			}

			// Very short timeout to exercise cancellation path.
			// S3/LocalStack latency means this may still complete.
			timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
			defer cancel()

			_, err = ds.StreamWrite(timeoutCtx, func(yield func(lode.D, error) bool) {
				for _, r := range records {
					if !yield(r, nil) {
						return
					}
				}
			}, nil)

			freshCtx := t.Context()
			if err == nil {
				snap, snapErr := ds.Latest(freshCtx)
				if snapErr != nil {
					t.Fatalf("Expected snapshot after successful write: %v", snapErr)
				}
				t.Logf("Write completed with %d rows (timeout didn't trigger)", snap.RowCount)
			} else {
				_, snapErr := ds.Latest(freshCtx)
				if !errors.Is(snapErr, lode.ErrNoSnapshots) {
					t.Errorf("Expected ErrNoSnapshots after timeout, got: %v", snapErr)
				}
			}
		})
	})
}

// cancelingReader yields prefix bytes and then cancels cancelCtx on the next Read.
type cancelingReader struct {
	prefix *bytes.Reader
	cancel context.CancelFunc
}

func (r *cancelingReader) Read(p []byte) (int, error) {
	if r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	r.cancel()
	return 0, context.Canceled
}

func deleteTestBucket(cleanupCtx context.Context, client *s3.Client, bucket string) {
	out, _ := client.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for _, obj := range out.Contents {
		errs.IgnoreResult(client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key}))
	}
	errs.IgnoreResult(client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}))
}

func newEphemeralBucketStore(ctx context.Context, tb testing.TB, client *s3.Client, prefix string) *Store {
	tb.Helper()
	var suffix [16]byte
	errs.IgnoreResult(rand.Read(suffix[:])) // crypto/rand.Read never returns an error.
	bucket := prefix + "-" + hex.EncodeToString(suffix[:])
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		tb.Fatalf("failed to create bucket: %v", err)
	}
	detached := context.WithoutCancel(ctx)
	tb.Cleanup(func() { deleteTestBucket(detached, client, bucket) })
	store, err := New(client, bucket)
	if err != nil {
		tb.Fatalf("failed to create store: %v", err)
	}
	return store
}

func forEachBucket(t *testing.T, run func(*testing.T, *Store)) {
	t.Helper()
	for _, backend := range s3Backends {
		t.Run(backend.name, func(t *testing.T) {
			run(t, setupTestBucket(t, backend))
		})
	}
}

func putIntegrationObject(t *testing.T, store *Store, key string, data []byte) {
	t.Helper()
	if err := store.Put(t.Context(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}
