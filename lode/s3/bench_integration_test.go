package s3

import (
	"context"
	"fmt"
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

// BenchmarkS3_WriteRoundTrip measures the cost of writing 3 records to S3,
// reading them back, and verifying the count. This exercises the full Dataset
// write → Latest → Read path against real S3-compatible backends.
func BenchmarkS3_WriteRoundTrip(b *testing.B) {
	skipBenchIfNoS3(b)

	backends := []struct {
		name      string
		newClient func(context.Context) (*s3.Client, error)
	}{
		{"LocalStack", newLocalStackClient},
		{"MinIO", newMinIOClient},
	}

	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			store := setupBenchBucket(b, backend.newClient)
			factory := func() (lode.Store, error) { return store, nil }

			records := lode.R(
				lode.D{"id": "1", "name": "alice"},
				lode.D{"id": "2", "name": "bob"},
				lode.D{"id": "3", "name": "carol"},
			)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dsID := lode.DatasetID(fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i))
				ds, err := lode.NewDataset(dsID, factory, lode.WithCodec(lode.NewJSONLCodec()))
				if err != nil {
					b.Fatal(err)
				}

				snap, err := ds.Write(b.Context(), records, lode.Metadata{"iter": fmt.Sprintf("%d", i)})
				if err != nil {
					b.Fatalf("Write failed: %v", err)
				}

				readBack, err := ds.Read(b.Context(), snap.ID)
				if err != nil {
					b.Fatalf("Read failed: %v", err)
				}

				if len(readBack) != 3 {
					b.Fatalf("expected 3 records, got %d", len(readBack))
				}
			}
		})
	}
}
