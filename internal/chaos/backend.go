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
	Store       lode.RangeStore
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
		// SingleProcess reads ranges exactly when the store it wraps does,
		// which is known only when it runs.
		store, ok := lode.SingleProcess(testutil.WithoutCAS(lode.NewMemory())).(lode.RangeStore)
		if !ok {
			return Backend{}, errors.New("chaos: SingleProcess over a memory store does not read ranges")
		}
		return Backend{Store: store, Release: keep, RetainsData: true}, nil
	},
	"fs": func(_ context.Context, dir, _ string) (Backend, error) {
		store, err := lode.FS(dir)()
		return Backend{Store: store, Release: keep}, err
	},
	"localstack": func(ctx context.Context, _, runID string) (Backend, error) {
		return dockerS3(ctx, dockerEndpoint("http://localhost:4566", "test"), runID)
	},
	"rustfs": func(ctx context.Context, _, runID string) (Backend, error) {
		return dockerS3(ctx, dockerEndpoint("http://localhost:9000", "rustfsadmin"), runID)
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

// s3Endpoint is how the harness reaches an S3-compatible service.
type s3Endpoint struct {
	url         string // empty for AWS S3
	region      string
	key, secret string
	pathStyle   bool
}

// dockerEndpoint is a local service of lode/s3/docker-compose.yaml, whose
// access key and secret are the same.
func dockerEndpoint(url, credential string) s3Endpoint {
	return s3Endpoint{url: url, region: "us-east-1", key: credential, secret: credential, pathStyle: true}
}

// client builds an S3 client for the endpoint.
func (e s3Endpoint) client(ctx context.Context) (*awss3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(e.region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(e.key, e.secret, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("chaos: load S3 configuration: %w", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if e.url != "" {
			o.BaseEndpoint = aws.String(e.url)
		}
		o.UsePathStyle = e.pathStyle
	}), nil
}

// s3Area is the part of a bucket a run owns: the whole bucket (an empty
// prefix), which release removes, or the objects under a prefix.
type s3Area struct {
	client *awss3.Client
	bucket string
	prefix string
}

// backend is the run's store over the area, released by emptying it.
func (a s3Area) backend() (Backend, error) {
	store, err := s3.New(a.client, s3.Config{Bucket: a.bucket, Prefix: a.prefix})
	if err != nil {
		return Backend{}, err
	}
	return Backend{Store: store, Release: a.empty, Lingering: idleHTTPConnection}, nil
}

// dockerS3 creates a bucket for the run on a local S3-compatible service and
// removes the bucket's objects and the bucket on release.
func dockerS3(ctx context.Context, endpoint s3Endpoint, runID string) (Backend, error) {
	client, err := endpoint.client(ctx)
	if err != nil {
		return Backend{}, err
	}
	area := s3Area{client: client, bucket: "lode-chaos-" + runID}
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(area.bucket)}); err != nil {
		return Backend{}, fmt.Errorf("chaos: create bucket %s at %s: %w", area.bucket, endpoint.url, err)
	}
	return area.backend()
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
	endpointConfig := s3Endpoint{url: endpoint, region: region, key: key, secret: secret, pathStyle: os.Getenv("CHAOS_S3_PATH_STYLE") == "1"}
	client, err := endpointConfig.client(ctx)
	if err != nil {
		return Backend{}, err
	}
	area := s3Area{client: client, bucket: bucket, prefix: "lode-chaos/" + runID + "/"}
	if err := area.refuseVersioned(ctx); err != nil {
		return Backend{}, err
	}
	return area.backend()
}

// refuseVersioned fails for a bucket with versioning enabled or suspended.
// A store that does not implement GetBucketVersioning (Cloudflare R2) has no
// object versioning, so its bucket is unversioned.
func (a s3Area) refuseVersioned(ctx context.Context) error {
	out, err := a.client.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: aws.String(a.bucket)})
	if notImplemented(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chaos: read versioning of bucket %s: %w", a.bucket, err)
	}
	if out.Status != "" {
		return fmt.Errorf("chaos: bucket %s has versioning %s; use an unversioned bucket so runs can be removed", a.bucket, out.Status)
	}
	return nil
}

// notImplemented reports whether err is the S3 error a store returns for an
// operation it does not implement.
func notImplemented(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotImplemented"
}

// empty deletes every object of the area and, when the area is the whole
// bucket, the bucket itself. It runs after the checks and returns every
// failure, so that residue left in a bucket is reported rather than ignored.
func (a s3Area) empty(ctx context.Context) error {
	var errs []error
	pages := awss3.NewListObjectsV2Paginator(a.client, &awss3.ListObjectsV2Input{Bucket: aws.String(a.bucket), Prefix: aws.String(a.prefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("chaos: list bucket %s for removal: %w", a.bucket, err)
		}
		for _, obj := range page.Contents {
			if _, err := a.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: obj.Key}); err != nil {
				errs = append(errs, fmt.Errorf("chaos: delete %s from bucket %s: %w", aws.ToString(obj.Key), a.bucket, err))
			}
		}
	}
	if a.prefix == "" && len(errs) == 0 {
		if _, err := a.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(a.bucket)}); err != nil {
			errs = append(errs, fmt.Errorf("chaos: delete bucket %s: %w", a.bucket, err))
		}
	}
	return errors.Join(errs...)
}
