package s3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"

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

	return newEphemeralBucketStore(ctx, b, client, "lode-bench")
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
// first two: one handle, opened before the timer starts, writes three
// records and reads them back in each iteration. The adapter records the
// pointer's version when it replaces the pointer but not when it creates
// it, so the second commit still reads the pointer; both commits precede
// the timer.
func BenchmarkS3_SteadyWrite(b *testing.B) {
	skipBenchIfNoS3(b)
	for _, backend := range benchBackends {
		b.Run(backend.name, func(b *testing.B) {
			root, requests := countedRoot(b, backend.newClient)
			ds, err := root.Records(b.Context(), "bench-steady", lode.JSONL[lode.D]())
			if err != nil {
				b.Fatal(err)
			}
			writeAndRead(b, ds, -2)
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

func writeAndRead(b *testing.B, ds *lode.Records[lode.D], i int) {
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
	requests := &requestCounter{calls: map[string]int{}}
	store := setupBenchBucket(b, func(ctx context.Context) (*s3.Client, error) {
		client, err := newClient(ctx)
		if err != nil {
			return nil, err
		}
		return s3.New(client.Options(), func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, requests.countOperation)
		}), nil
	})
	return lode.Open(lode.Use(store)), requests
}

// requestCounter counts the S3 requests of a store by operation.
type requestCounter struct {
	mu    sync.Mutex
	calls map[string]int
}

func (c *requestCounter) countOperation(stack *middleware.Stack) error {
	// Count a logical SDK call once, before any transport retries.
	metric := map[string]string{"PutObject": "put", "GetObject": "get", "HeadObject": "head", "DeleteObject": "delete", "ListObjectsV2": "list"}
	if op, ok := metric[stack.ID()]; ok {
		c.mu.Lock()
		c.calls[op]++
		c.mu.Unlock()
	}
	return nil
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
