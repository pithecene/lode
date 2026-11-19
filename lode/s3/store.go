// Package s3 provides an S3-compatible storage adapter for Lode.
//
// This adapter targets AWS S3 and S3-compatible object stores. Key prefixing
// is composed via lode.Prefixed(s3.Bucket(client, "my-bucket"), "my-prefix").
//
// # Contract Compliance
//
// This adapter implements CONTRACT_STORAGE.md obligations:
//   - Put: Uses atomic or multipart path based on payload size.
//     Atomic (≤5GB): Spools to temp file, then PutObject with If-None-Match.
//     Provides atomic no-overwrite guarantee with O(1) memory usage.
//     Multipart (>5GB): Uses CompleteMultipartUpload with If-None-Match for
//     atomic no-overwrite guarantee. Preflight check optimizes fail-fast behavior.
//   - Get/Exists/Delete: Standard ErrNotFound semantics
//   - List: Full pagination support, returns all matching keys
//   - ListPrefixes: Delimiter-bounded shallow prefix enumeration via CommonPrefixes
//   - ReadRange: True range reads via HTTP Range header
//   - ReaderAt: Concurrent-safe random access reads
//
// # S3-Specific Limits
//
//   - Atomic path threshold: 5GB (S3 PutObject limit)
//   - Maximum object size: 5TB (S3 multipart limit)
//   - Part size: 5MB minimum, adaptive for objects >50GB to stay under 10,000 parts
//
// # Backend Compatibility for Conditional Multipart Completion
//
// The multipart path (>5GB) uses If-None-Match on CompleteMultipartUpload for
// atomic no-overwrite guarantees. This feature requires backend support:
//
//	| Backend       | Conditional Completion | Status                |
//	|---------------|------------------------|-----------------------|
//	| Cloudflare R2 | Supported              | ✅ Production verified |
//	| AWS S3        | Supported              | ✅ Expected compatible |
//	| Hetzner       | Ignored                | ⚠️ Best-effort only    |
//	| RustFS        | Supported              | ⚠️ Test backend only   |
//	| MinIO         | Unknown                | ⚠️ Not tested          |
//	| LocalStack    | Unknown                | ⚠️ Test backend only   |
//
// For backends marked ⚠️: these are used for integration testing only. If
// deploying against a backend not listed as verified, test with large uploads
// (>5GB) before relying on the multipart path, or ensure single-writer
// semantics at the application level.
//
// Hetzner Object Storage completes a multipart upload over an existing object
// despite If-None-Match, so above 5GB the preflight check is the only
// protection there and callers MUST ensure single-writer semantics
// (CONTRACT_STORAGE §Put). Below 5GB, If-None-Match and If-Match on PutObject
// hold under concurrent writers; a writer that loses an If-Match race
// sometimes receives 500 instead of 412, and the SDK's retry of it then
// receives 412.
//
// # Consistency
//
// AWS S3 provides strong read-after-write consistency (since Dec 2020).
// Other S3-compatible backends (MinIO, LocalStack, R2) may have different
// consistency guarantees — consult their documentation. Commit semantics
// rely on manifest presence: writers MUST write data objects before the manifest.
package s3

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 names a single-part object by its MD5
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
	"github.com/pithecene/lode/lode"
)

// S3 multipart upload constraints.
const (
	minPartSize    = 5 * 1024 * 1024               // 5MB
	maxPartSize    = 5 * 1024 * 1024 * 1024        // 5GB
	maxParts       = 10000                         // 10,000 parts max
	maxObjectSize  = 5 * 1024 * 1024 * 1024 * 1024 // 5TB
	maxAtomicPutSz = 5 * 1024 * 1024 * 1024        // 5GB PutObject limit
)

// API defines the subset of the S3 client interface used by the store.
// This enables testing with mock implementations.
type API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type s3Config struct{}

// Option configures an S3 Store created via New or Bucket.
type Option interface {
	apply(*s3Config) error
}

// Store implements lode.RangeStore and lode.PrefixLister using an S3-compatible
// backend.
type Store struct {
	client     API
	bucket     string
	createTemp func() (*os.File, error) // temp file factory for Put spooling
	versions   *versions                // CompareAndSwap's record of the versions it replaced
}

var (
	_ lode.RangeStore   = (*Store)(nil)
	_ lode.PrefixLister = (*Store)(nil)
)

// New creates a new S3 store targeting bucket with the given pre-configured
// client. To scope all keys under a prefix within bucket, wrap Bucket or
// lode.Use(store) with lode.Prefixed.
//
// Note on SSE-KMS and SSE-C: on buckets using SSE-KMS or SSE-C encryption,
// single-part object ETags are not MD5 digests of the plaintext payload, so a
// lost-response SDK retry of PutObject cannot confirm payload identity via
// HeadObject and returns lode.ErrPathExists instead of nil.
//
// Example:
//
//	cfg, err := config.LoadDefaultConfig(ctx)
//	client := awss3.NewFromConfig(cfg)
//	store, err := s3.New(client, "my-bucket")
func New(client API, bucket string, opts ...Option) (*Store, error) {
	if validate.Nil(client) {
		return nil, fmt.Errorf("s3: %w: client is required", lode.ErrInvalidArgument)
	}
	if bucket == "" {
		return nil, fmt.Errorf("s3: %w: bucket is required", lode.ErrInvalidArgument)
	}
	var cfg s3Config
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("s3: %w: option must not be nil", lode.ErrInvalidArgument)
		}
		if err := opt.apply(&cfg); err != nil {
			return nil, err
		}
	}

	return &Store{
		client:     client,
		bucket:     bucket,
		createTemp: func() (*os.File, error) { return os.CreateTemp("", "lode-s3-*") },
		versions:   newVersions(),
	}, nil
}

// Bucket returns a factory for an S3 RangeStore targeting bucket over client,
// for lode.Open and lode.OpenReadOnly. It performs no I/O until a root opens,
// and New's validation errors are recorded by the root rather than returned
// here:
//
//	root := lode.Open(lode.Prefixed(s3.Bucket(client, "warehouse"), "prod"))
func Bucket(client API, bucket string, opts ...Option) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) {
		s, err := New(client, bucket, opts...)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
}

func shouldUseAtomicPath(size int64) bool {
	return size <= maxAtomicPutSz
}

// Put writes data to the given path.
// Returns ErrPathExists if the path already exists.
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	tmpFile, err := s.createTemp()
	if err != nil {
		return fmt.Errorf("s3: creating temp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	digest := md5.New() //nolint:gosec // S3 names a single-part object by its MD5; this is a comparison, not a security hash
	size, err := io.Copy(tmpFile, io.TeeReader(r, digest))
	if err != nil {
		return fmt.Errorf("s3: writing temp file: %w", err)
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("s3: seeking temp file: %w", err)
	}

	if shouldUseAtomicPath(size) {
		return s.putAtomicFromFile(ctx, fullKey, tmpFile, size, hex.EncodeToString(digest.Sum(nil)))
	}
	return s.putMultipartFromFile(ctx, fullKey, tmpFile, size)
}

func (s *Store) putAtomicFromFile(ctx context.Context, fullKey string, file io.ReadSeeker, size int64, md5Hex string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          file,
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		if hasErrorCode(err, slices.Concat(preconditionCodes, conflictCodes)...) {
			if s.ownUpload(ctx, fullKey, size, md5Hex) {
				return nil
			}
			return lode.ErrPathExists
		}
		return fmt.Errorf("s3: put object: %w", err)
	}
	return nil
}

func (s *Store) ownUpload(ctx context.Context, fullKey string, size int64, md5Hex string) bool {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return false
	}
	return aws.ToInt64(out.ContentLength) == size && strings.Trim(aws.ToString(out.ETag), `"`) == md5Hex
}

func (s *Store) putMultipartFromFile(ctx context.Context, fullKey string, file io.ReaderAt, size int64) error {
	if size > maxObjectSize {
		return fmt.Errorf("s3: object size %d exceeds maximum %d (5TB)", size, maxObjectSize)
	}

	partSize := int64(minPartSize)
	if size > int64(minPartSize)*maxParts {
		partSize = (size + maxParts - 1) / maxParts
	}

	exists, err := s.exists(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("checking existence: %w", err)
	}
	if exists {
		return lode.ErrPathExists
	}

	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return fmt.Errorf("s3: create multipart upload: %w", err)
	}
	uploadID := aws.ToString(createResp.UploadId)

	var completedParts []types.CompletedPart

	//nolint:contextcheck // Intentionally uses background context for cleanup resilience
	abortUpload := func() {
		abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(fullKey),
			UploadId: aws.String(uploadID),
		})
	}

	var offset int64
	partNum := int32(0)
	for offset < size {
		partNum++
		thisPartSize := partSize
		if remaining := size - offset; remaining < thisPartSize {
			thisPartSize = remaining
		}

		partReader := io.NewSectionReader(file, offset, thisPartSize)
		uploadResp, uploadErr := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(fullKey),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNum),
			Body:          partReader,
			ContentLength: aws.Int64(thisPartSize),
		})
		if uploadErr != nil {
			abortUpload()
			return fmt.Errorf("s3: upload part %d: %w", partNum, uploadErr)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNum),
		})

		offset += thisPartSize
	}

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		abortUpload()
		if hasErrorCode(err, slices.Concat(preconditionCodes, conflictCodes)...) {
			return lode.ErrPathExists
		}
		return fmt.Errorf("s3: complete multipart upload: %w", err)
	}

	return nil
}

// Get retrieves data from the given path.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		return nil, fmt.Errorf("s3: get object: %w", err)
	}

	return out.Body, nil
}

// Exists checks whether a path exists.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return false, err
	}

	return s.exists(ctx, fullKey)
}

// List returns all paths under the given prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := s.validatePrefix(prefix)
	if err != nil {
		return nil, err
	}

	var keys []string
	var continuationToken *string

	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list objects: %w", err)
		}

		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

// ListPrefixes returns immediate child prefixes (each ending in "/") beneath
// prefix using S3 ListObjectsV2 Delimiter="/", handling pagination.
func (s *Store) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix, err := s.validatePrefix(prefix)
	if err != nil {
		return nil, err
	}

	prefixes := []string{}
	var continuationToken *string

	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(fullPrefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list prefixes: %w", err)
		}

		for _, cp := range out.CommonPrefixes {
			if cp.Prefix != nil {
				prefixes = append(prefixes, *cp.Prefix)
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	slices.Sort(prefixes)
	return slices.Compact(prefixes), nil
}

// Delete removes the path if it exists.
func (s *Store) Delete(ctx context.Context, key string) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}
	s.versions.forget(fullKey)

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return fmt.Errorf("s3: delete object: %w", err)
	}

	return nil
}

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See lode.ConditionalWriter for semantics.
func (s *Store) CompareAndSwap(ctx context.Context, key, expected, replacement string) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}
	if cond, ok := s.knownCondition(fullKey, expected); ok {
		err := s.putConditional(ctx, fullKey, replacement, cond)
		if err == nil {
			return nil
		}
		if !stalePrecondition(err) {
			return s.mapCASError(err)
		}
	}
	return s.compareAndSwapByRead(ctx, fullKey, expected, replacement)
}

func stalePrecondition(err error) bool {
	return hasErrorCode(err, slices.Concat(preconditionCodes, notFoundCodes)...)
}

type condition struct {
	etag string
}

func (s *Store) knownCondition(fullKey, expected string) (condition, bool) {
	if expected == "" {
		return condition{}, true
	}
	etag, ok := s.versions.etagFor(fullKey, expected)
	return condition{etag: etag}, ok
}

func (s *Store) compareAndSwapByRead(ctx context.Context, fullKey, expected, replacement string) error {
	out, getErr := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if getErr != nil {
		if !isNotFound(getErr) {
			return fmt.Errorf("s3: get object for CAS: %w", getErr)
		}
		if expected != "" {
			return lode.ErrSnapshotConflict
		}
		return s.mapCASError(s.putConditional(ctx, fullKey, replacement, condition{}))
	}
	defer closer(out.Body)()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("s3: reading CAS content: %w", err)
	}
	if string(data) != expected {
		return lode.ErrSnapshotConflict
	}
	return s.mapCASError(s.putConditional(ctx, fullKey, replacement, condition{etag: aws.ToString(out.ETag)}))
}

func (s *Store) putConditional(ctx context.Context, fullKey, replacement string, cond condition) error {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          strings.NewReader(replacement),
		ContentLength: aws.Int64(int64(len(replacement))),
	}
	if cond.etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(ifMatchValue(cond.etag))
	}
	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		s.versions.forget(fullKey)
		return fmt.Errorf("s3: CAS put: %w", err)
	}
	if cond.etag == "" {
		s.versions.forget(fullKey)
		return nil
	}
	s.versions.record(fullKey, replacement, aws.ToString(out.ETag))
	return nil
}

func ifMatchValue(etag string) string {
	return strings.Trim(etag, `"`)
}

func (s *Store) mapCASError(err error) error {
	if err == nil {
		return nil
	}
	if hasErrorCode(err, slices.Concat(preconditionCodes, conflictCodes, notFoundCodes)...) {
		return lode.ErrSnapshotConflict
	}
	return err
}

// ReadRange reads a byte range from the given path.
func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if !storepath.RangeValid(offset, length) {
		return nil, lode.ErrInvalidPath
	}

	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		exists, err := s.exists(ctx, fullKey)
		if err != nil {
			return nil, fmt.Errorf("checking existence: %w", err)
		}
		if !exists {
			return nil, lode.ErrNotFound
		}
		return []byte{}, nil
	}

	end := offset + length - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, end)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("s3: range read: %w", err)
	}
	defer closer(out.Body)()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: reading range body: %w", err)
	}

	return data, nil
}

// ReaderAt returns an io.ReaderAt for random access reads.
func (s *Store) ReaderAt(ctx context.Context, key string) (io.ReaderAt, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, lode.ErrNotFound
		}
		return nil, fmt.Errorf("s3: head object: %w", err)
	}

	return &readerAt{
		store:   s,
		bucket:  s.bucket,
		key:     fullKey,
		baseCtx: ctx,
	}, nil
}

type readerAt struct {
	store   *Store
	bucket  string
	key     string
	baseCtx context.Context
}

func (r *readerAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("s3: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}

	end := off + int64(len(p)) - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	out, err := r.store.client.GetObject(r.baseCtx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("s3: range read: %w", err)
	}
	defer closer(out.Body)()

	n, err = io.ReadFull(out.Body, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (s *Store) exists(ctx context.Context, fullKey string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3: head object: %w", err)
	}
	return true, nil
}

func (s *Store) validateKey(key string) (string, error) {
	k, ok := storepath.Key(key)
	if !ok {
		return "", lode.ErrInvalidPath
	}
	return k, nil
}

func (s *Store) validatePrefix(prefix string) (string, error) {
	p, ok := storepath.Prefix(prefix)
	if !ok {
		return "", lode.ErrInvalidPath
	}
	return p, nil
}

var (
	preconditionCodes = []string{"PreconditionFailed", "412"}
	conflictCodes     = []string{"ConditionalRequestConflict", "409"}
	notFoundCodes     = []string{"NotFound", "NoSuchKey", "404"}
)

func hasErrorCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && slices.Contains(codes, apiErr.ErrorCode())
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nsb *types.NoSuchBucket
	return errors.As(err, &nsk) || errors.As(err, &nsb) || hasErrorCode(err, notFoundCodes...)
}
