package chaos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/s3"
)

// Backend is a store shared by every writer and reader of one run, and the
// release of whatever the run created on it. Release reports what it failed
// to remove, so a run cannot pass while leaving data in a shared bucket.
// RetainsData marks a store that keeps the data in process memory, whose
// heap therefore grows with the run by design. Lingering, when set, reports
// whether a goroutine's stack belongs to the backend's own machinery, which
// legitimately outlives the run, rather than to a leak.
type Backend struct {
	Store       lode.Store
	Release     func(ctx context.Context) error
	RetainsData bool
	Lingering   func(stack string) bool
}

func keep(context.Context) error { return nil }

// openBackend builds the named backend for a run. dir is a scratch
// directory for the filesystem backend; runID keeps runs on a shared bucket
// apart.
type openBackend func(ctx context.Context, dir, runID string) (Backend, error)

// backends are the stores the harness knows. LocalStack and RustFS are the
// services of lode/s3/docker-compose.yaml; "s3" is any S3-compatible store
// configured through CHAOS_S3_* environment variables (see s3FromEnv).
// "single-process" is lode.SingleProcess over a memory store without CAS:
// every writer of a run is a goroutine of this process, which is the scope
// within which SingleProcess serializes commits.
var backends = map[string]openBackend{
	"memory": func(context.Context, string, string) (Backend, error) {
		return Backend{Store: lode.NewMemory(), Release: keep, RetainsData: true}, nil
	},
	"single-process": func(context.Context, string, string) (Backend, error) {
		return Backend{Store: lode.SingleProcess(testutil.WithoutCAS(lode.NewMemory())), Release: keep, RetainsData: true}, nil
	},
	"fs": func(_ context.Context, dir, _ string) (Backend, error) {
		store, err := lode.NewFSFactory(dir)()
		return Backend{Store: store, Release: keep}, err
	},
	"localstack": func(ctx context.Context, _, runID string) (Backend, error) {
		return dockerS3(ctx, "http://localhost:4566", "test", "test", runID)
	},
	"rustfs": func(ctx context.Context, _, runID string) (Backend, error) {
		return dockerS3(ctx, "http://localhost:9000", "rustfsadmin", "rustfsadmin", runID)
	},
	"s3": func(ctx context.Context, _, runID string) (Backend, error) {
		return s3FromEnv(ctx, runID)
	},
}

// BackendNames lists the known backends in a stable order.
func BackendNames() []string { return slices.Sorted(maps.Keys(backends)) }

// OpenBackend opens the named backend for one run.
func OpenBackend(ctx context.Context, name, dir, runID string) (Backend, error) {
	if err := requireKnown("backend", name, BackendNames()); err != nil {
		return Backend{}, err
	}
	return backends[name](ctx, dir, runID)
}

// pooledConnectionFrames are the functions in which the HTTP client's
// connection pool keeps a goroutine per open connection: two for an
// HTTP/1.1 connection, one for an HTTP/2 connection, which an HTTPS
// endpoint such as AWS S3 or R2 negotiates.
var pooledConnectionFrames = []string{
	"net/http.(*persistConn).readLoop",
	"net/http.(*persistConn).writeLoop",
	"net/http/internal/http2.(*ClientConn).readLoop",
}

// idleHTTPConnection reports whether a goroutine serves a connection that
// the HTTP client keeps open for reuse. The AWS SDK owns its client and
// pools connections privately, so such goroutines outlive a run on an S3
// backend by design. A response body left open also holds a connection, but
// it retains memory with every request, which the heap check reports.
func idleHTTPConnection(stack string) bool {
	return slices.ContainsFunc(pooledConnectionFrames, func(frame string) bool { return strings.Contains(stack, frame) })
}

// dockerS3 creates a bucket for the run on a local S3-compatible service and
// removes the bucket's objects and the bucket on release.
func dockerS3(ctx context.Context, endpoint, key, secret, runID string) (Backend, error) {
	client, err := s3Client(ctx, endpoint, "us-east-1", key, secret, true)
	if err != nil {
		return Backend{}, err
	}
	bucket := "lode-chaos-" + runID
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return Backend{}, fmt.Errorf("chaos: create bucket %s at %s: %w", bucket, endpoint, err)
	}
	store, err := s3.New(client, s3.Config{Bucket: bucket})
	if err != nil {
		return Backend{}, err
	}
	return Backend{
		Store:     store,
		Release:   func(ctx context.Context) error { return emptyBucket(ctx, client, bucket, "") },
		Lingering: idleHTTPConnection,
	}, nil
}

// s3FromEnv opens an existing bucket on any S3-compatible store (AWS S3,
// Cloudflare R2, ...) under a prefix unique to the run, which is removed on
// release. Required: CHAOS_S3_BUCKET, CHAOS_S3_ACCESS_KEY_ID,
// CHAOS_S3_SECRET_ACCESS_KEY, and CHAOS_S3_REGION when CHAOS_S3_ENDPOINT is
// unset (AWS). Optional: CHAOS_S3_ENDPOINT (R2 and other S3-compatible
// stores; the region then defaults to "auto"), CHAOS_S3_PATH_STYLE ("1" for
// path-style addressing). A versioned bucket is refused: deleting its objects
// would only add delete markers, and every run would leave its data behind.
func s3FromEnv(ctx context.Context, runID string) (Backend, error) {
	bucket, key, secret := os.Getenv("CHAOS_S3_BUCKET"), os.Getenv("CHAOS_S3_ACCESS_KEY_ID"), os.Getenv("CHAOS_S3_SECRET_ACCESS_KEY")
	if bucket == "" || key == "" || secret == "" {
		return Backend{}, errors.New("chaos: backend s3 requires CHAOS_S3_BUCKET, CHAOS_S3_ACCESS_KEY_ID and CHAOS_S3_SECRET_ACCESS_KEY")
	}
	endpoint, region := os.Getenv("CHAOS_S3_ENDPOINT"), os.Getenv("CHAOS_S3_REGION")
	switch {
	case region == "" && endpoint == "":
		return Backend{}, errors.New("chaos: backend s3 without CHAOS_S3_ENDPOINT (AWS) requires CHAOS_S3_REGION")
	case region == "":
		region = "auto"
	}
	client, err := s3Client(ctx, endpoint, region, key, secret, os.Getenv("CHAOS_S3_PATH_STYLE") == "1")
	if err != nil {
		return Backend{}, err
	}
	if err := refuseVersioned(ctx, client, bucket); err != nil {
		return Backend{}, err
	}
	prefix := "lode-chaos/" + runID + "/"
	store, err := s3.New(client, s3.Config{Bucket: bucket, Prefix: prefix})
	if err != nil {
		return Backend{}, err
	}
	return Backend{
		Store:     store,
		Release:   func(ctx context.Context) error { return emptyBucket(ctx, client, bucket, prefix) },
		Lingering: idleHTTPConnection,
	}, nil
}

// refuseVersioned fails for a bucket with versioning enabled or suspended.
// A store that does not implement GetBucketVersioning (Cloudflare R2) has no
// object versioning, so its bucket is unversioned.
func refuseVersioned(ctx context.Context, client *awss3.Client, bucket string) error {
	out, err := client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if notImplemented(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chaos: read versioning of bucket %s: %w", bucket, err)
	}
	if out.Status != "" {
		return fmt.Errorf("chaos: bucket %s has versioning %s; use an unversioned bucket so runs can be removed", bucket, out.Status)
	}
	return nil
}

// notImplemented reports whether err is the S3 error a store returns for an
// operation it does not implement.
func notImplemented(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotImplemented"
}

func s3Client(ctx context.Context, endpoint, region, key, secret string, pathStyle bool) (*awss3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("chaos: load S3 configuration: %w", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = pathStyle
	}), nil
}

// emptyBucket deletes every object under prefix and, when prefix is empty,
// the bucket itself. It runs after the checks and returns every failure, so
// that residue left in a bucket is reported rather than ignored.
func emptyBucket(ctx context.Context, client *awss3.Client, bucket, prefix string) error {
	var errs []error
	pages := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("chaos: list bucket %s for removal: %w", bucket, err)
		}
		for _, obj := range page.Contents {
			if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key}); err != nil {
				errs = append(errs, fmt.Errorf("chaos: delete %s from bucket %s: %w", aws.ToString(obj.Key), bucket, err))
			}
		}
	}
	if prefix == "" && len(errs) == 0 {
		if _, err := client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			errs = append(errs, fmt.Errorf("chaos: delete bucket %s: %w", bucket, err))
		}
	}
	return errors.Join(errs...)
}
