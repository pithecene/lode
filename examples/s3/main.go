// Example: S3 Storage Adapter
//
// This example demonstrates using the S3 storage adapter with Lode.
//
// Requirements:
// - An S3-compatible service (LocalStack or RustFS)
// - The bucket must exist (e.g., `aws --endpoint-url=http://localhost:4566 s3 mb s3://lode-example`)
// - Environment variables (optional, defaults provided below)
//
// Run with: go run ./examples/s3
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/s3"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	bucket := getenv("LODE_S3_BUCKET", "lode-example")
	endpoint := getenv("LODE_S3_ENDPOINT", "http://localhost:4566")
	region := getenv("LODE_S3_REGION", "us-east-1")
	accessKey := getenv("LODE_S3_ACCESS_KEY", "test")
	secretKey := getenv("LODE_S3_SECRET_KEY", "test")

	// Unique prefix to avoid collisions between runs.
	prefix := fmt.Sprintf("examples/s3/%d/", time.Now().UnixNano())

	// Create S3 client using AWS SDK directly.
	// For AWS S3, use config.LoadDefaultConfig(ctx) without custom endpoint.
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure for S3-compatible service (LocalStack).
	// For AWS S3, omit BaseEndpoint and UsePathStyle.
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Verify connectivity before proceeding. S3-compatible services
	// must be running (e.g., `task s3:up`).
	_, err = client.ListBuckets(ctx, &awss3.ListBucketsInput{})
	if err != nil {
		fmt.Printf("S3 service not reachable at %s\n", endpoint)
		fmt.Println("Start services first: task s3:up")
		fmt.Println("Skipping example.")
		return nil //nolint:nilerr // intentional: skip example gracefully when S3 is unreachable
	}

	// Open a root over the S3 store. s3.Factory performs no I/O; a
	// configuration error is returned when the dataset opens.
	root := lode.Open(s3.Factory(client, s3.Config{
		Bucket: bucket,
		Prefix: prefix,
	}))

	ds, err := root.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to open dataset: %w", err)
	}

	records := lode.R(
		lode.D{"id": 1, "event": "login", "user": "alice"},
		lode.D{"id": 2, "event": "click", "user": "bob"},
	)

	snapshot, err := ds.Write(ctx, records, lode.D{"source": "s3-example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("S3 prefix: %s\n", prefix)
	fmt.Printf("Files in manifest: %d\n", len(snapshot.Files))

	datasets, err := root.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	return nil
}

func getenv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
