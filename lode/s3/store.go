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
//	| RustFS        | Supported              | ⚠️ Test backend only   |
//	| MinIO         | Unknown                | ⚠️ Not tested          |
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
	"context"
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
	"github.com/pithecene/lode/lode"
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
	// If set, all keys are prefixed with this value (with a trailing slash
	// added if missing). It follows the key rules of CONTRACT_STORAGE §Put:
	// New returns lode.ErrInvalidPath for a prefix that escapes the root or
	// contains a backslash.
	Prefix string
}

// Store implements lode.Store using an S3-compatible backend.
type Store struct {
	client     API
	bucket     string
	prefix     string
	createTemp func() (*os.File, error) // temp file factory for Put spooling
	versions   *versions                // CompareAndSwap's record of the versions it replaced
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

	// The configured prefix becomes part of every key, so it obeys the same
	// rules as a key (storepath): no escape, no backslash. It is normalized to
	// end in "/" so callers' keys sit beneath it.
	prefix, ok := storepath.Prefix(cfg.Prefix)
	if !ok {
		return nil, fmt.Errorf("s3: invalid prefix %q: %w", cfg.Prefix, lode.ErrInvalidPath)
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Store{
		client:     client,
		bucket:     cfg.Bucket,
		prefix:     prefix,
		createTemp: func() (*os.File, error) { return os.CreateTemp("", "lode-s3-*") },
		versions:   newVersions(),
	}, nil
}

// Factory returns a factory for a Store over client, for lode.Open: it
// performs no I/O until a root opens, and New's error is recorded by the
// root rather than returned here.
//
//	root := lode.Open(s3.Factory(client, s3.Config{Bucket: "my-bucket"}))
func Factory(client API, cfg Config) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) {
		s, err := New(client, cfg)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
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
		if hasErrorCode(err, preconditionCodes...) {
			return lode.ErrPathExists // object already exists
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
		return fmt.Errorf("checking existence: %w", err)
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
		if hasErrorCode(err, slices.Concat(preconditionCodes, conflictCodes)...) {
			return lode.ErrPathExists // another writer completed first
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
	// The object's recorded version, if any, no longer exists.
	s.versions.forget(fullKey)

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
// The conditional PutObject decides every outcome: If-None-Match: * creates
// the object, and If-Match replaces exactly the version that was compared.
// When the precondition is already known, CompareAndSwap writes without
// reading the object: to create it (expected is empty), or to replace the
// version that this store last wrote by replacement when its content is
// expected. Otherwise, or when that write's precondition fails, it first
// reads the object to learn its content and ETag. It maps
// PreconditionFailed and ConditionalRequestConflict to ErrSnapshotConflict.
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
		// The object is not the version assumed: it exists although expected
		// is empty (it may be empty itself), or another writer replaced or
		// deleted it.
	}
	return s.compareAndSwapByRead(ctx, fullKey, expected, replacement)
}

// stalePrecondition reports whether a conditional write failed because the
// object is not the version assumed. S3 refuses a precondition with 412, and
// answers If-Match on an object that no longer exists with 404 (NoSuchKey);
// a missing bucket is not such a refusal.
func stalePrecondition(err error) bool {
	return hasErrorCode(err, slices.Concat(preconditionCodes, notFoundCodes)...)
}

// condition is the precondition of a conditional PutObject: If-Match on
// etag, or If-None-Match: * when etag is empty.
type condition struct {
	etag string
}

// knownCondition returns the precondition under which replacement can be
// written without first reading the object, if there is one: absence when
// expected is empty, or the ETag of the version this store last wrote by
// replacement when that version's content is expected.
func (s *Store) knownCondition(fullKey, expected string) (condition, bool) {
	if expected == "" {
		return condition{}, true
	}
	etag, ok := s.versions.etagFor(fullKey, expected)
	return condition{etag: etag}, ok
}

// compareAndSwapByRead reads the object to compare its content with
// expected and to capture its ETag, then writes under that precondition.
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

// putConditional writes replacement under cond. A replacement (If-Match)
// records the version it wrote; a creation records nothing, because objects
// created through CompareAndSwap and never replaced, such as tombstones,
// would otherwise accumulate records without bound. After any failure the
// object's version is unknown (a lost response may hide a completed write),
// so the record is dropped.
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
		input.IfMatch = aws.String(cond.etag)
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

// mapCASError maps S3 conditional-write refusals to ErrSnapshotConflict,
// including If-Match on an object that another writer deleted (404). Any
// other failure is returned as putConditional reported it.
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
// Returns ErrNotFound if the path does not exist.
// Returns ErrInvalidPath for negative offset/length, overflow, or invalid paths.
// If offset is beyond EOF, returns empty slice.
// If range extends beyond EOF, returns available bytes.
// If length is 0, returns empty slice without making a request.
func (s *Store) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if !storepath.RangeValid(offset, length) {
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
			return nil, fmt.Errorf("checking existence: %w", err)
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
		return false, fmt.Errorf("s3: head object: %w", err)
	}
	return true, nil
}

// validateKey applies the shared key rules (storepath.Key) and returns the
// full bucket key under the store prefix.
func (s *Store) validateKey(key string) (string, error) {
	k, ok := storepath.Key(key)
	if !ok {
		return "", lode.ErrInvalidPath
	}
	return s.prefix + k, nil
}

// validatePrefix applies the shared prefix rules (storepath.Prefix) and
// returns the full bucket prefix for a listing.
func (s *Store) validatePrefix(prefix string) (string, error) {
	p, ok := storepath.Prefix(prefix)
	if !ok {
		return "", lode.ErrInvalidPath
	}
	return s.prefix + p, nil
}

// S3 spells each conditional-write refusal two ways.
var (
	preconditionCodes = []string{"PreconditionFailed", "412"}         // If-Match / If-None-Match not satisfied
	conflictCodes     = []string{"ConditionalRequestConflict", "409"} // concurrent conditional writers
	notFoundCodes     = []string{"NotFound", "NoSuchKey", "404"}
)

// hasErrorCode reports whether err is an S3 API error carrying one of codes.
func hasErrorCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && slices.Contains(codes, apiErr.ErrorCode())
}

// isNotFound checks if an error indicates the object was not found.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nsb *types.NoSuchBucket
	return errors.As(err, &nsk) || errors.As(err, &nsb) || hasErrorCode(err, notFoundCodes...)
}
