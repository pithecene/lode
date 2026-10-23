package s3

// MockS3Client is the in-memory test double for the S3 API. It is compiled
// only into this package's tests, so it is not part of the adapter's API
// (docs/V1_API_DECISIONS.md D7).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

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

// objectETag returns a deterministic ETag for the given data. It is derived
// from the content, as S3's ETag of a single-part upload is, so two versions
// of an object carry different ETags and If-Match distinguishes them.
func objectETag(data []byte) string {
	return fmt.Sprintf("\"%x\"", sha256.Sum256(data))
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
			// S3 answers If-Match on an absent object with 404.
			return nil, &types.NoSuchKey{}
		}
		if objectETag(existing) != ifMatch {
			return nil, &smithyAPIError{code: "PreconditionFailed", message: "etag mismatch"}
		}
	}

	m.objects[key] = data
	return &s3.PutObjectOutput{ETag: aws.String(objectETag(data))}, nil
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

	// S3 rejects part numbers outside 1–maxParts. Enforcing the limit keeps
	// a part loop that fails to advance from accumulating parts without
	// bound in this in-memory double.
	if partNum < 1 || partNum > maxParts {
		return nil, &smithyAPIError{code: "InvalidArgument", message: "part number must be an integer between 1 and 10000"}
	}

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
