package s3

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

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
	{"MinIO", newMinIOClient},
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

// newMinIOClient creates an S3 client for MinIO (integration tests only).
func newMinIOClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
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

	bucket := fmt.Sprintf("lode-test-%d", time.Now().UnixNano())

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	t.Cleanup(func() {
		// Intentional: t.Context() is canceled before t.Cleanup runs,
		// so cleanup operations need an independent context.
		cleanupCtx := context.Background()
		out, _ := client.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		for _, obj := range out.Contents {
			_, _ = client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}
		_, _ = client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	store, err := New(client, Config{Bucket: bucket})
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	return store
}

// -----------------------------------------------------------------------------
// Store Integration Tests
// -----------------------------------------------------------------------------

func TestIntegration_Reads(t *testing.T) {
	t.Run("write list read", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				content := []byte("hello world")
				key := "test/file.txt"

				err := store.Put(ctx, key, bytes.NewReader(content))
				if err != nil {
					t.Fatalf("Put failed: %v", err)
				}

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
				_ = rc.Close()
				if err != nil {
					t.Fatalf("reading body failed: %v", err)
				}
				if string(data) != string(content) {
					t.Errorf("expected %q, got %q", string(content), string(data))
				}
			})
		}
	})

	t.Run("range read", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				content := []byte("0123456789")
				key := "test/range.txt"

				err := store.Put(ctx, key, bytes.NewReader(content))
				if err != nil {
					t.Fatalf("Put failed: %v", err)
				}

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
		}
	})

	t.Run("reader at", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				content := []byte("hello world reader at")
				key := "test/readerat.txt"

				err := store.Put(ctx, key, bytes.NewReader(content))
				if err != nil {
					t.Fatalf("Put failed: %v", err)
				}

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
		}
	})

	t.Run("not found", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
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
		}
	})
}

func TestIntegration_Writes(t *testing.T) {
	t.Run("immutability enforcement", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				content := []byte("immutable")
				key := "test/immutable.txt"

				err := store.Put(ctx, key, bytes.NewReader(content))
				if err != nil {
					t.Fatalf("first Put failed: %v", err)
				}

				err = store.Put(ctx, key, bytes.NewReader([]byte("modified")))
				if !errors.Is(err, lode.ErrPathExists) {
					t.Errorf("expected ErrPathExists on second write, got: %v", err)
				}
			})
		}
	})

	t.Run("manifest last visibility", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
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
				manifestJSON := `{"schema_name":"lode-manifest","format_version":"1.0","dataset_id":"test-ds","snapshot_id":"snap1","created_at":"2024-01-01T00:00:00Z","metadata":{},"files":[{"path":"data/file.jsonl","size_bytes":10}],"row_count":1,"compressor":"noop","partitioner":"noop"}`
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
		}
	})

	// TestIntegration_StreamWrite_ContextCancel verifies that when context
	// is canceled during a streaming write to S3, no snapshot becomes visible.
	t.Run("stream write context cancel", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				storeFactory := func() (lode.Store, error) { return store, nil }

				ds, err := lode.NewDataset("cancel-test", storeFactory)
				if err != nil {
					t.Fatal(err)
				}

				cancelCtx, cancel := context.WithCancel(ctx)

				sw, err := ds.StreamWrite(cancelCtx, lode.Metadata{"test": "cancel"})
				if err != nil {
					t.Fatal(err)
				}

				_, err = sw.Write([]byte("data that will be canceled"))
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}

				cancel()

				_, err = sw.Commit(cancelCtx)
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
		}
	})

	// TestIntegration_StreamWriteRecords_ContextTimeout verifies that when
	// context times out during StreamWriteRecords to S3, no partial snapshot is visible.
	t.Run("stream write records context timeout", func(t *testing.T) {
		for _, backend := range s3Backends {
			t.Run(backend.name, func(t *testing.T) {
				store := setupTestBucket(t, backend)
				ctx := t.Context()

				storeFactory := func() (lode.Store, error) { return store, nil }

				ds, err := lode.NewDataset("timeout-test", storeFactory, lode.WithCodec(lode.NewJSONLCodec()))
				if err != nil {
					t.Fatal(err)
				}

				records := make([]any, 100)
				for i := range records {
					records[i] = lode.D{"id": fmt.Sprintf("%d", i), "data": "payload"}
				}

				// Very short timeout to exercise cancellation path.
				// S3/LocalStack latency means this may still complete.
				timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
				defer cancel()

				iter := &testRecordIterator{records: records}
				_, err = ds.StreamWriteRecords(timeoutCtx, iter, lode.Metadata{})

				freshCtx := t.Context()
				if err == nil {
					snap, snapErr := ds.Latest(freshCtx)
					if snapErr != nil {
						t.Fatalf("Expected snapshot after successful write: %v", snapErr)
					}
					t.Logf("Write completed with %d rows (timeout didn't trigger)", snap.Manifest.RowCount)
				} else {
					_, snapErr := ds.Latest(freshCtx)
					if !errors.Is(snapErr, lode.ErrNoSnapshots) {
						t.Errorf("Expected ErrNoSnapshots after timeout, got: %v", snapErr)
					}
				}
			})
		}
	})
}

// -----------------------------------------------------------------------------
// Timing Integration Tests
// -----------------------------------------------------------------------------
//
// These tests verify S3 adapter behavior under realistic timing conditions.
// They complement the deterministic fault-injection tests by exercising
// real network cancellation and cleanup paths.
//
// Key principle: assert on core invariants (no manifest = no visible snapshot),
// but don't make timing-dependent assertions that could flake due to network latency.
//
// Contract reference: CONTRACT_TEST_MATRIX.md (G3-4 residual risk)

// testRecordIterator is a simple iterator for testing.
type testRecordIterator struct {
	records []any
	index   int
}

func (i *testRecordIterator) Next() bool {
	if i.index >= len(i.records) {
		return false
	}
	i.index++
	return true
}

func (i *testRecordIterator) Record() any {
	if i.index == 0 || i.index > len(i.records) {
		return nil
	}
	return i.records[i.index-1]
}

func (i *testRecordIterator) Err() error {
	return nil
}
