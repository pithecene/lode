package s3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/lode/lode"
)

// skipBenchIfNoS3 skips benchmark execution when S3 test services are unavailable.
func skipBenchIfNoS3(b *testing.B) {
	b.Helper()
	if !*flagIntegration {
		b.Skip("skipping S3 benchmark; use -integration to enable")
	}
}

// setupBenchBucket creates a unique bucket for benchmarking and registers cleanup.
func setupBenchBucket(b *testing.B, newClient func(context.Context) (*s3.Client, error)) *Store {
	b.Helper()

	ctx := b.Context()
	client, err := newClient(ctx)
	if err != nil {
		b.Fatalf("failed to create S3 client: %v", err)
	}

	bucket := fmt.Sprintf("lode-bench-%d", time.Now().UnixNano())

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		b.Fatalf("failed to create bucket: %v", err)
	}

	b.Cleanup(func() {
		// Intentional: b.Context() is canceled before b.Cleanup runs,
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
		b.Fatalf("failed to create store: %v", err)
	}

	return store
}

// benchRecords are the records that each benchmark iteration writes.
var benchRecords = lode.R(
	lode.D{"id": "1", "name": "alice"},
	lode.D{"id": "2", "name": "bob"},
	lode.D{"id": "3", "name": "carol"},
)

// benchBackends are the S3-compatible services that the benchmarks run
// against.
var benchBackends = []struct {
	name      string
	newClient func(context.Context) (*s3.Client, error)
}{
	{"LocalStack", newLocalStackClient},
	{"RustFS", newRustFSClient},
}

// BenchmarkS3_FirstWrite measures a dataset's first commit and one read:
// each iteration opens a new dataset, writes three records, and reads them
// back. It reports the S3 requests per iteration beside the allocations,
// because nearly all allocations occur in the AWS SDK, once per request.
func BenchmarkS3_FirstWrite(b *testing.B) {
	skipBenchIfNoS3(b)
	for _, backend := range benchBackends {
		b.Run(backend.name, func(b *testing.B) {
			root, requests := countedRoot(b, backend.newClient)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dsID := lode.DatasetID(fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i))
				ds, err := root.Records(b.Context(), dsID, lode.JSONL[lode.D]())
				if err != nil {
					b.Fatal(err)
				}
				writeAndRead(b, ds, i)
			}
			b.StopTimer()
			requests.report(b)
		})
	}
}

// BenchmarkS3_SteadyWrite measures the commits that follow a dataset's
// first: one handle, opened before the timer starts, writes three records
// and reads them back in each iteration.
func BenchmarkS3_SteadyWrite(b *testing.B) {
	skipBenchIfNoS3(b)
	for _, backend := range benchBackends {
		b.Run(backend.name, func(b *testing.B) {
			root, requests := countedRoot(b, backend.newClient)
			ds, err := root.Records(b.Context(), "bench-steady", lode.JSONL[lode.D]())
			if err != nil {
				b.Fatal(err)
			}
			writeAndRead(b, ds, -1)
			requests.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writeAndRead(b, ds, i)
			}
			b.StopTimer()
			requests.report(b)
		})
	}
}

func writeAndRead(b *testing.B, ds lode.Records[lode.D], i int) {
	b.Helper()
	snap, err := ds.Write(b.Context(), benchRecords, lode.D{"iter": fmt.Sprintf("%d", i)})
	if err != nil {
		b.Fatalf("Write failed: %v", err)
	}
	readBack, err := ds.Read(b.Context(), snap.ID)
	if err != nil {
		b.Fatalf("Read failed: %v", err)
	}
	if len(readBack) != len(benchRecords) {
		b.Fatalf("expected %d records, got %d", len(benchRecords), len(readBack))
	}
}

// countedRoot opens a root over a new bucket whose S3 requests are counted.
func countedRoot(b *testing.B, newClient func(context.Context) (*s3.Client, error)) (*lode.Root, *requestCounter) {
	b.Helper()
	base := setupBenchBucket(b, newClient)
	requests := &requestCounter{API: base.client, calls: map[string]int{}}
	store, err := New(requests, Config{Bucket: base.bucket})
	if err != nil {
		b.Fatal(err)
	}
	return lode.Open(lode.Use(store)), requests
}

// requestCounter counts the S3 requests of a store by operation.
type requestCounter struct {
	API
	mu    sync.Mutex
	calls map[string]int
}

func (c *requestCounter) count(op string) {
	c.mu.Lock()
	c.calls[op]++
	c.mu.Unlock()
}

func (c *requestCounter) reset() {
	c.mu.Lock()
	clear(c.calls)
	c.mu.Unlock()
}

// report adds the requests per iteration, in total and by operation, to the
// benchmark's output.
func (c *requestCounter) report(b *testing.B) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for op, n := range c.calls {
		total += n
		b.ReportMetric(float64(n)/float64(b.N), op+"/op")
	}
	b.ReportMetric(float64(total)/float64(b.N), "requests/op")
}

func (c *requestCounter) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.count("put")
	return c.API.PutObject(ctx, in, opts...)
}

func (c *requestCounter) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.count("get")
	return c.API.GetObject(ctx, in, opts...)
}

func (c *requestCounter) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.count("head")
	return c.API.HeadObject(ctx, in, opts...)
}

func (c *requestCounter) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.count("delete")
	return c.API.DeleteObject(ctx, in, opts...)
}

func (c *requestCounter) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.count("list")
	return c.API.ListObjectsV2(ctx, in, opts...)
}
