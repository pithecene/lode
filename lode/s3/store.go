// Package s3 provides an S3-compatible storage adapter for Lode.
//
// This adapter targets AWS S3 and S3-compatible object stores.
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
//	| MinIO         | Unknown                | ⚠️ Test backend only   |
//	| LocalStack    | Unknown                | ⚠️ Test backend only   |
//
// For backends marked ⚠️: these are used for integration testing only. If
// deploying against a backend not listed as verified, test with large uploads
// (>5GB) before relying on the multipart path, or ensure single-writer
// semantics at the application level.
//
// # Consistency
//
// AWS S3 provides strong read-after-write consistency (since Dec 2020).
// Other S3-compatible backends (MinIO, LocalStack, R2) may have different
// consistency guarantees — consult their documentation. Commit semantics
// rely on manifest presence: writers MUST write data objects before the manifest.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/pithecene-io/lode/lode"
)

// S3 multipart upload constraints.
const (
	// minPartSize is the minimum part size for S3 multipart uploads (except last part).
	minPartSize = 5 * 1024 * 1024 // 5MB

	// maxPartSize is the maximum part size for S3 multipart uploads.
	maxPartSize = 5 * 1024 * 1024 * 1024 // 5GB

	// maxParts is the maximum number of parts allowed in an S3 multipart upload.
	maxParts = 10000

	// maxObjectSize is the maximum object size for S3 (5TB per AWS documentation).
	// This is an S3 service limit, independent of part size calculations.
	maxObjectSize = 5 * 1024 * 1024 * 1024 * 1024 // 5TB
)

// maxAtomicPutSize is the threshold for atomic vs multipart Put routing.
// Objects ≤ this size use PutObject with If-None-Match (atomic no-overwrite).
// Objects > this size use multipart upload (best-effort no-overwrite via preflight check).
// Set to 5GB (the S3 PutObject limit) to maximize atomic upload coverage.
const maxAtomicPutSize = 5 * 1024 * 1024 * 1024 // 5GB

// maxReadRangeLength is the maximum length for ReadRange to prevent overflow
// when converting int64 to int on 32-bit platforms.
const maxReadRangeLength = int64(math.MaxInt)

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

// Config holds configuration for the S3 store.
type Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// Prefix is an optional key prefix for all operations.
	// If set, all keys are prefixed with this value (with a trailing slash added if missing).
	Prefix string
}

// Store implements lode.Store using an S3-compatible backend.
type Store struct {
	client     API
	bucket     string
	prefix     string
	createTemp func() (*os.File, error) // temp file factory for Put spooling
}

// New creates a new S3 store with the given client and configuration.
//
// The client must be pre-configured with credentials, region, and endpoint.
// Use github.com/aws/aws-sdk-go-v2/config to load configuration.
//
// Example:
//
//	cfg, err := config.LoadDefaultConfig(ctx)
//	client := s3.NewFromConfig(cfg)
//	store, err := s3store.New(client, s3store.Config{Bucket: "my-bucket"})
func New(client API, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3: client is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}

	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Store{
		client:     client,
		bucket:     cfg.Bucket,
		prefix:     prefix,
		createTemp: func() (*os.File, error) { return os.CreateTemp("", "lode-s3-*") },
	}, nil
}

// shouldUseAtomicPath returns true if the given size should use the atomic Put path.
// This is a pure function for routing decisions, testable without large files.
func shouldUseAtomicPath(size int64) bool {
	return size <= maxAtomicPutSize
}

// Put writes data to the given path.
// Returns ErrPathExists if the path already exists.
// Returns ErrInvalidPath for empty or escaping paths.
//
// # Routing and Atomicity (per CONTRACT_STORAGE.md)
//
// Atomic path (≤5GB): Spools to temp file, then uses PutObject with
// If-None-Match for atomic no-overwrite protection. O(1) memory usage.
// Duplicate writes return ErrPathExists.
//
// Multipart path (>5GB): Uses preflight HeadObject check then multipart
// upload. Provides best-effort no-overwrite protection with a TOCTOU window.
// Single-writer or external coordination is required for guaranteed
// no-overwrite semantics on this path.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	// Spool to temp file to determine size and enable seekable upload.
	// This provides O(1) memory usage regardless of upload size.
	tmpFile, err := s.createTemp()
	if err != nil {
		return fmt.Errorf("s3: creating temp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	// Copy to temp file, tracking size
	size, err := io.Copy(tmpFile, r)
	if err != nil {
		return fmt.Errorf("s3: writing temp file: %w", err)
	}

	// Seek to start for upload
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("s3: seeking temp file: %w", err)
	}

	// Route based on size
	if shouldUseAtomicPath(size) {
		return s.putAtomicFromFile(ctx, fullKey, tmpFile, size)
	}
	return s.putMultipartFromFile(ctx, fullKey, tmpFile, size)
}

// putAtomicFromFile implements atomic Put via PutObject with If-None-Match.
// Supports uploads up to 5GB (S3 PutObject limit).
// The file is passed directly to the SDK for memory-efficient streaming.
func (s *Store) putAtomicFromFile(ctx context.Context, fullKey string, file io.ReadSeeker, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          file,
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		// Check for PreconditionFailed (object already exists)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := apiErr.ErrorCode()
			if code == "PreconditionFailed" || code == "412" {
				return lode.ErrPathExists
			}
		}
		return fmt.Errorf("s3: put object: %w", err)
	}
	return nil
}

// putMultipartFromFile implements multipart upload for objects > 5GB.
// Reads parts directly from file using io.SectionReader for memory efficiency.
//
// Uses conditional completion (If-None-Match) for atomic no-overwrite guarantee,
// matching the atomic path. The preflight existence check is retained as an
// optimization to fail fast before uploading parts.
func (s *Store) putMultipartFromFile(ctx context.Context, fullKey string, file io.ReaderAt, size int64) error {
	// Check S3 size limit (5TB max)
	if size > maxObjectSize {
		return fmt.Errorf("s3: object size %d exceeds maximum %d (5TB)", size, maxObjectSize)
	}

	// Calculate adaptive part size to stay under 10,000 parts.
	// For objects ≤50GB, use 5MB parts. For larger objects, scale up.
	partSize := int64(minPartSize)
	if size > int64(minPartSize)*maxParts {
		// Need larger parts to fit within maxParts limit
		// Round up: ceil(size / maxParts)
		partSize = (size + maxParts - 1) / maxParts
	}

	// Preflight existence check (optimization to fail fast before uploading parts).
	// The actual atomicity guarantee comes from If-None-Match on CompleteMultipartUpload.
	exists, err := s.exists(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("s3: checking existence: %w", err)
	}
	if exists {
		return lode.ErrPathExists
	}

	// Create multipart upload
	createResp, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return fmt.Errorf("s3: create multipart upload: %w", err)
	}
	uploadID := aws.ToString(createResp.UploadId)

	// Track completed parts for CompleteMultipartUpload
	var completedParts []types.CompletedPart

	// Helper to abort on error. Uses background context to ensure cleanup
	// even if the original context was canceled (per CONTRACT_STORAGE.md).
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

	// Upload parts directly from file using SectionReader (no memory buffering)
	var offset int64
	partNum := int32(0)
	for offset < size {
		partNum++
		thisPartSize := partSize
		if remaining := size - offset; remaining < thisPartSize {
			thisPartSize = remaining
		}

		// SectionReader provides a view into the file without copying
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

	// Complete multipart upload with conditional no-overwrite (If-None-Match).
	// ASSUMPTION: Backend supports If-None-Match on CompleteMultipartUpload.
	// Verified on AWS S3; untested on other S3-compatible backends (MinIO, LocalStack, R2).
	// See package doc "Backend Compatibility for Conditional Multipart Completion" for details.
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
		// Check for conditional write failures (object already exists)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := apiErr.ErrorCode()
			// PreconditionFailed (412) - standard conditional failure
			// ConditionalRequestConflict (409) - S3 conflict during conditional request
			if code == "PreconditionFailed" || code == "412" ||
				code == "ConditionalRequestConflict" || code == "409" {
				return lode.ErrPathExists
			}
		}
		return fmt.Errorf("s3: complete multipart upload: %w", err)
	}

	return nil
}

// Get retrieves data from the given path.
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath for empty or escaping paths.
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
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return false, err
	}

	return s.exists(ctx, fullKey)
}

// List returns all paths under the given prefix.
// Pagination is handled automatically; all matching keys are returned.
// Returns ErrInvalidPath for escaping prefixes.
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
				// Strip the store prefix to return relative keys
				relKey := strings.TrimPrefix(*obj.Key, s.prefix)
				keys = append(keys, relKey)
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

// Delete removes the path if it exists.
// Safe to call on missing paths (idempotent).
// Returns ErrInvalidPath for empty or escaping paths.
func (s *Store) Delete(ctx context.Context, key string) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		// S3 DeleteObject is idempotent; it doesn't error on missing keys
		return fmt.Errorf("s3: delete object: %w", err)
	}

	return nil
}

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See lode.ConditionalWriter for semantics.
//
// Uses GetObject to read current content + capture ETag, then PutObject with
// If-Match to atomically replace. Maps PreconditionFailed/ConditionalRequestConflict
// to ErrSnapshotConflict.
func (s *Store) CompareAndSwap(ctx context.Context, key, expected, replacement string) error {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return err
	}

	// Read current content and capture ETag.
	out, getErr := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})

	if getErr != nil {
		if !isNotFound(getErr) {
			return fmt.Errorf("s3: get object for CAS: %w", getErr)
		}
		// Path does not exist.
		if expected != "" {
			return lode.ErrSnapshotConflict
		}
		// First commit: create with If-None-Match to guard against races.
		_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(fullKey),
			Body:          strings.NewReader(replacement),
			ContentLength: aws.Int64(int64(len(replacement))),
			IfNoneMatch:   aws.String("*"),
		})
		if putErr != nil {
			return s.mapCASError(putErr)
		}
		return nil
	}
	defer closer(out.Body)()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("s3: reading CAS content: %w", err)
	}

	if string(data) != expected {
		return lode.ErrSnapshotConflict
	}

	// Content matches: replace with If-Match to guard against concurrent updates.
	etag := aws.ToString(out.ETag)
	_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          strings.NewReader(replacement),
		ContentLength: aws.Int64(int64(len(replacement))),
		IfMatch:       aws.String(etag),
	})
	if putErr != nil {
		return s.mapCASError(putErr)
	}
	return nil
}

// mapCASError maps S3 precondition errors to ErrSnapshotConflict.
func (s *Store) mapCASError(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "PreconditionFailed" || code == "412" ||
			code == "ConditionalRequestConflict" || code == "409" {
			return lode.ErrSnapshotConflict
		}
	}
	return fmt.Errorf("s3: CAS put: %w", err)
}

// ReadRange reads a byte range from the given path.
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath for negative offset/length, overflow, or invalid paths.
// If offset is beyond EOF, returns empty slice.
// If range extends beyond EOF, returns available bytes.
// If length is 0, returns empty slice without making a request.
func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	// Validate offset and length per CONTRACT_STORAGE.md
	if offset < 0 || length < 0 || length > maxReadRangeLength {
		return nil, lode.ErrInvalidPath
	}
	if offset > math.MaxInt64-length {
		return nil, lode.ErrInvalidPath
	}

	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	// Zero-length read: verify existence then return empty slice.
	// Contract requires ErrNotFound for missing paths regardless of length.
	if length == 0 {
		exists, err := s.exists(ctx, fullKey)
		if err != nil {
			return nil, fmt.Errorf("s3: checking existence: %w", err)
		}
		if !exists {
			return nil, lode.ErrNotFound
		}
		return []byte{}, nil
	}

	// S3 Range header format: "bytes=start-end" (inclusive)
	// end = offset + length - 1
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
		// Check for InvalidRange (offset beyond EOF)
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
// Returns ErrNotFound if the path does not exist.
// The returned ReaderAt supports concurrent reads at different offsets.
// Callers should close the ReaderAt if it implements io.Closer.
func (s *Store) ReaderAt(ctx context.Context, key string) (io.ReaderAt, error) {
	fullKey, err := s.validateKey(key)
	if err != nil {
		return nil, err
	}

	// Verify the object exists
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

// readerAt implements io.ReaderAt using S3 range reads.
// It is safe for concurrent use.
type readerAt struct {
	store   *Store
	bucket  string
	key     string
	baseCtx context.Context
}

// ReadAt implements io.ReaderAt.
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
		// Check for InvalidRange (offset beyond EOF)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("s3: range read: %w", err)
	}
	defer closer(out.Body)()

	n, err = io.ReadFull(out.Body, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// Partial read (requested range extends beyond EOF)
		err = io.EOF
	}
	return n, err
}

// exists checks if an object exists (internal helper).
func (s *Store) exists(ctx context.Context, fullKey string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// validateKey validates and returns the full key for file operations.
func (s *Store) validateKey(key string) (string, error) {
	if key == "" {
		return "", lode.ErrInvalidPath
	}

	// Normalize the key
	cleaned := path.Clean(key)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", lode.ErrInvalidPath
	}
	// Remove leading slash
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" {
		return "", lode.ErrInvalidPath
	}

	return s.prefix + cleaned, nil
}

// validatePrefix validates and returns the full prefix for list operations.
func (s *Store) validatePrefix(prefix string) (string, error) {
	if prefix == "" {
		return s.prefix, nil
	}

	cleaned := path.Clean(prefix)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", lode.ErrInvalidPath
	}
	if cleaned == "." {
		return s.prefix, nil
	}
	// Remove leading slash
	cleaned = strings.TrimPrefix(cleaned, "/")
	// path.Clean strips a trailing "/", but "a/" and "a" are different string
	// prefixes (CONTRACT_STORAGE §List): only the former excludes "a2/x".
	// A prefix that reduces to the root ("/") stays the root.
	if cleaned != "" && strings.HasSuffix(prefix, "/") {
		cleaned += "/"
	}

	return s.prefix + cleaned, nil
}

// isNotFound checks if an error indicates the object was not found.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nsb *types.NoSuchBucket
	if errors.As(err, &nsb) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchKey" || code == "404"
	}
	return false
}

// -----------------------------------------------------------------------------
// Mock S3 Client for Testing
// -----------------------------------------------------------------------------

// multipartUpload tracks an in-progress multipart upload.
type multipartUpload struct {
	parts map[int32][]byte
}

// MockS3Client is a test double for API.
type MockS3Client struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	uploads  map[string]*multipartUpload // uploadID -> upload
	uploadID int

	// Call counters for test assertions
	PutObjectCalls             int
	CreateMultipartUploadCalls int
	AbortMultipartUploadCalls  int

	// UploadPartFailOnCall causes UploadPart to fail on the Nth call.
	// Set to 0 to disable (default). Set to 1 to fail on first part, 2 for second, etc.
	UploadPartFailOnCall int
	uploadPartCalls      int
}

// NewMockS3Client creates a new mock S3 client for testing.
func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string][]byte),
		uploads: make(map[string]*multipartUpload),
	}
}

// ResetCounts resets call counters for test isolation.
func (m *MockS3Client) ResetCounts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PutObjectCalls = 0
	m.CreateMultipartUploadCalls = 0
	m.AbortMultipartUploadCalls = 0
	m.uploadPartCalls = 0
}

// objectETag returns a deterministic ETag for the given data.
func objectETag(data []byte) string {
	return fmt.Sprintf("\"%x\"", len(data))
}

// PutObject implements API.PutObject for testing.
func (m *MockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(params.Key)
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.PutObjectCalls++

	// Handle If-None-Match: "*" (conditional write for immutability)
	if aws.ToString(params.IfNoneMatch) == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "object already exists"}
		}
	}

	// Handle If-Match: <etag> (conditional update for CAS)
	if ifMatch := aws.ToString(params.IfMatch); ifMatch != "" {
		existing, exists := m.objects[key]
		if !exists {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "object does not exist"}
		}
		if objectETag(existing) != ifMatch {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "etag mismatch"}
		}
	}

	m.objects[key] = data
	return &s3.PutObjectOutput{}, nil
}

// GetObject implements API.GetObject for testing.
func (m *MockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.RLock()
	data, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, &types.NoSuchKey{}
	}

	// Handle range requests
	if params.Range != nil {
		rangeStr := aws.ToString(params.Range)
		var start, end int64
		_, _ = fmt.Sscanf(rangeStr, "bytes=%d-%d", &start, &end)

		if start >= int64(len(data)) {
			return nil, &smithyAPIError{code: "InvalidRange"}
		}

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		data = data[start : end+1]
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
		ETag: aws.String(objectETag(data)),
	}, nil
}

// HeadObject implements API.HeadObject for testing.
func (m *MockS3Client) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.RLock()
	_, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, &types.NoSuchKey{}
	}

	return &s3.HeadObjectOutput{}, nil
}

// CreateMultipartUpload implements API.CreateMultipartUpload for testing.
func (m *MockS3Client) CreateMultipartUpload(_ context.Context, params *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CreateMultipartUploadCalls++
	m.uploadID++
	uploadID := fmt.Sprintf("upload-%d", m.uploadID)

	m.uploads[uploadID] = &multipartUpload{
		parts: make(map[int32][]byte),
	}

	return &s3.CreateMultipartUploadOutput{
		Bucket:   params.Bucket,
		Key:      params.Key,
		UploadId: aws.String(uploadID),
	}, nil
}

// UploadPart implements API.UploadPart for testing.
func (m *MockS3Client) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	uploadID := aws.ToString(params.UploadId)
	partNum := aws.ToInt32(params.PartNumber)

	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Simulate failure on Nth call (for testing abort path)
	m.uploadPartCalls++
	if m.UploadPartFailOnCall > 0 && m.uploadPartCalls >= m.UploadPartFailOnCall {
		return nil, &smithyAPIError{code: "InternalError", message: "simulated upload part failure"}
	}

	upload, exists := m.uploads[uploadID]
	if !exists {
		return nil, &smithyAPIError{code: "NoSuchUpload", message: "upload not found"}
	}

	upload.parts[partNum] = data

	// Generate a fake ETag
	etag := fmt.Sprintf("\"%d-%d\"", partNum, len(data))
	return &s3.UploadPartOutput{ETag: aws.String(etag)}, nil
}

// CompleteMultipartUpload implements API.CompleteMultipartUpload for testing.
func (m *MockS3Client) CompleteMultipartUpload(_ context.Context, params *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	uploadID := aws.ToString(params.UploadId)
	key := aws.ToString(params.Key)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle If-None-Match: "*" (conditional write for immutability)
	if aws.ToString(params.IfNoneMatch) == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "object already exists"}
		}
	}

	upload, exists := m.uploads[uploadID]
	if !exists {
		return nil, &smithyAPIError{code: "NoSuchUpload", message: "upload not found"}
	}

	// Assemble parts in order
	var assembled []byte
	for i := int32(1); i <= int32(len(upload.parts)); i++ {
		assembled = append(assembled, upload.parts[i]...)
	}

	m.objects[key] = assembled
	delete(m.uploads, uploadID)

	return &s3.CompleteMultipartUploadOutput{}, nil
}

// AbortMultipartUpload implements API.AbortMultipartUpload for testing.
func (m *MockS3Client) AbortMultipartUpload(_ context.Context, params *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	uploadID := aws.ToString(params.UploadId)

	m.mu.Lock()
	m.AbortMultipartUploadCalls++
	delete(m.uploads, uploadID)
	m.mu.Unlock()

	return &s3.AbortMultipartUploadOutput{}, nil
}

// DeleteObject implements API.DeleteObject for testing.
func (m *MockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(params.Key)

	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()

	return &s3.DeleteObjectOutput{}, nil
}

// ListObjectsV2 implements API.ListObjectsV2 for testing.
func (m *MockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(params.Prefix)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var contents []types.Object
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			k := key
			contents = append(contents, types.Object{Key: &k})
		}
	}

	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

// smithyAPIError implements smithy.APIError for testing.
type smithyAPIError struct {
	code    string
	message string
}

func (e *smithyAPIError) Error() string {
	return e.message
}

func (e *smithyAPIError) ErrorCode() string {
	return e.code
}

func (e *smithyAPIError) ErrorMessage() string {
	return e.message
}

func (e *smithyAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}
