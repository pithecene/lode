package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3api "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/lode/lode"
)

// -----------------------------------------------------------------------------
// Unit tests for S3 store
// These use the mock client and don't require real S3/LocalStack/MinIO.
// -----------------------------------------------------------------------------

func TestNew(t *testing.T) {
	t.Run("requires client", func(t *testing.T) {
		_, err := New(nil, Config{Bucket: "test"})
		if err == nil {
			t.Error("expected error for nil client")
		}
	})

	t.Run("requires bucket", func(t *testing.T) {
		_, err := New(NewMockS3Client(), Config{})
		if err == nil {
			t.Error("expected error for empty bucket")
		}
	})

	t.Run("prefix normalization", func(t *testing.T) {
		tests := []struct {
			prefix   string
			expected string
		}{
			{"", ""},
			{"foo", "foo/"},
			{"foo/", "foo/"},
			{"foo/bar", "foo/bar/"},
			{"foo/bar/", "foo/bar/"},
			{"/foo", "foo/"},
			{"foo//bar", "foo/bar/"},
		}

		for _, tt := range tests {
			store, err := New(NewMockS3Client(), Config{Bucket: "test", Prefix: tt.prefix})
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			if store.prefix != tt.expected {
				t.Errorf("prefix %q: expected %q, got %q", tt.prefix, tt.expected, store.prefix)
			}
		}
	})

	// The configured prefix is part of every key, so a prefix that a key could
	// not be is refused at construction rather than composed into object keys.
	t.Run("prefix rejected", func(t *testing.T) {
		for _, prefix := range []string{`..`, `../x`, `a\b`, `..\x`} {
			_, err := New(NewMockS3Client(), Config{Bucket: "test", Prefix: prefix})
			if !errors.Is(err, lode.ErrInvalidPath) {
				t.Errorf("prefix %q: got %v, want ErrInvalidPath", prefix, err)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// Put tests
// -----------------------------------------------------------------------------

func TestStore_Put(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	t.Run("err path exists", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		// First write should succeed
		err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
		if err != nil {
			t.Fatalf("first Put failed: %v", err)
		}

		// Second write to same path should return ErrPathExists
		err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("world")))
		if !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("expected ErrPathExists, got: %v", err)
		}
	})

	t.Run("err invalid path empty", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.Put(ctx, "", bytes.NewReader([]byte("hello")))
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for empty path, got: %v", err)
		}
	})

	t.Run("err invalid path escaping", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		tests := []string{
			"..",
			"../foo",
			"foo/../..",
			"foo/../../bar",
		}

		for _, path := range tests {
			err := store.Put(ctx, path, bytes.NewReader([]byte("hello")))
			if !errors.Is(err, lode.ErrInvalidPath) {
				t.Errorf("path %q: expected ErrInvalidPath, got: %v", path, err)
			}
		}
	})

	t.Run("temp file cleanup on success", func(t *testing.T) {
		tracker := newTempFileTracker(t.TempDir())

		ctx := t.Context()
		store, err := New(NewMockS3Client(), Config{Bucket: "test"})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		store.createTemp = tracker.createTemp

		// Perform successful Put
		err = store.Put(ctx, "test.txt", bytes.NewReader([]byte("hello")))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Verify all tracked temp files were cleaned up
		if tracker.count.Load() == 0 {
			t.Error("expected at least one temp file to be created")
		}
		tracker.assertAllCleaned(t)
	})

	t.Run("temp file cleanup on failure", func(t *testing.T) {
		tracker := newTempFileTracker(t.TempDir())

		ctx := t.Context()
		mock := NewMockS3Client()
		store, err := New(mock, Config{Bucket: "test"})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		store.createTemp = tracker.createTemp

		// First Put succeeds
		_ = store.Put(ctx, "existing.txt", bytes.NewReader([]byte("first")))

		// Second Put fails with ErrPathExists
		_ = store.Put(ctx, "existing.txt", bytes.NewReader([]byte("second")))

		// Verify all tracked temp files were cleaned up (including both Put calls)
		if tracker.count.Load() < 2 {
			t.Errorf("expected at least 2 temp files created, got %d", tracker.count.Load())
		}
		tracker.assertAllCleaned(t)
	})
}

// -----------------------------------------------------------------------------
// Put routing decision tests (per CONTRACT_STORAGE.md)
//
// Production threshold: maxAtomicPutSize = 5GB
// - size ≤ 5GB: atomic path (PutObject with If-None-Match)
// - size > 5GB: multipart path (preflight HeadObject + multipart upload)
//
// These tests verify the pure routing function with synthetic sizes,
// avoiding the need for 5GB test files.
// -----------------------------------------------------------------------------

func TestStore_PutAtomic(t *testing.T) {
	t.Run("should use atomic path", func(t *testing.T) {
		tests := []struct {
			name     string
			size     int64
			expected bool
		}{
			{"zero bytes", 0, true},
			{"1 byte", 1, true},
			{"1 MB", 1 * 1024 * 1024, true},
			{"1 GB", 1 * 1024 * 1024 * 1024, true},
			{"5 GB - 1", maxAtomicPutSize - 1, true},
			{"exactly 5 GB", maxAtomicPutSize, true},
			{"5 GB + 1", maxAtomicPutSize + 1, false},
			{"10 GB", 10 * 1024 * 1024 * 1024, false},
			{"5 TB", maxObjectSize, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := shouldUseAtomicPath(tt.size)
				if got != tt.expected {
					t.Errorf("shouldUseAtomicPath(%d) = %v, want %v", tt.size, got, tt.expected)
				}
			})
		}
	})

	t.Run("put atomic from file success", func(t *testing.T) {
		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Create a small temp file
		data := []byte("hello atomic path")
		tmpFile, err := os.CreateTemp(t.TempDir(), "test-*")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer closer(tmpFile)()

		if _, err := tmpFile.Write(data); err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}
		if _, err := tmpFile.Seek(0, 0); err != nil {
			t.Fatalf("failed to seek temp file: %v", err)
		}

		// Call internal atomic path directly
		err = store.putAtomicFromFile(ctx, "atomic-test.txt", tmpFile, int64(len(data)))
		if err != nil {
			t.Fatalf("putAtomicFromFile failed: %v", err)
		}

		// Verify PutObject was called with If-None-Match
		mock.mu.RLock()
		putCalls := mock.PutObjectCalls
		stored := mock.objects["atomic-test.txt"]
		mock.mu.RUnlock()

		if putCalls != 1 {
			t.Errorf("expected 1 PutObject call, got %d", putCalls)
		}
		if !bytes.Equal(data, stored) {
			t.Error("stored data does not match original")
		}
	})

	t.Run("put atomic duplicate returns err path exists", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		// Small file uses atomic path with If-None-Match
		data := []byte("hello")

		// First write should succeed
		err := store.Put(ctx, "atomic-dup.txt", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("first Put failed: %v", err)
		}

		// Second write should return ErrPathExists (atomic detection via PreconditionFailed)
		err = store.Put(ctx, "atomic-dup.txt", bytes.NewReader(data))
		if !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("expected ErrPathExists, got: %v", err)
		}
	})

	t.Run("S3 limits are correct", func(t *testing.T) {
		// Verify S3 limits are correctly defined per AWS documentation
		if minPartSize != 5*1024*1024 {
			t.Errorf("minPartSize should be 5MB, got %d", minPartSize)
		}
		if maxPartSize != 5*1024*1024*1024 {
			t.Errorf("maxPartSize should be 5GB, got %d", maxPartSize)
		}
		if maxParts != 10000 {
			t.Errorf("maxParts should be 10000, got %d", maxParts)
		}
		// S3's max object size is 5TB (service limit, not computed from parts)
		const fiveTB = int64(5) * 1024 * 1024 * 1024 * 1024
		if maxObjectSize != fiveTB {
			t.Errorf("maxObjectSize should be %d (5TB), got %d", fiveTB, maxObjectSize)
		}
		if maxAtomicPutSize != 5*1024*1024*1024 {
			t.Errorf("maxAtomicPutSize should be 5GB, got %d", maxAtomicPutSize)
		}
	})

	t.Run("put adaptive part sizing", func(t *testing.T) {
		// Test that part size calculation works correctly for large objects.
		// We can't actually create 50GB+ files, but we can verify the math.

		// For size <= 50GB (minPartSize * maxParts), partSize = minPartSize
		size50GB := int64(minPartSize) * maxParts
		expectedPartSize := int64(minPartSize)
		if size50GB/expectedPartSize > maxParts {
			t.Errorf("50GB with 5MB parts would exceed maxParts")
		}

		// For size = 100GB, need larger parts
		size100GB := int64(100) * 1024 * 1024 * 1024
		neededPartSize := (size100GB + maxParts - 1) / maxParts // ceil division
		numParts := (size100GB + neededPartSize - 1) / neededPartSize
		if numParts > maxParts {
			t.Errorf("100GB adaptive sizing would use %d parts (max %d)", numParts, maxParts)
		}
		if neededPartSize < minPartSize {
			t.Errorf("100GB part size %d is below minimum %d", neededPartSize, minPartSize)
		}
	})
}

// -----------------------------------------------------------------------------
// Internal path tests - verify atomic and multipart paths work correctly
// by calling internal methods directly with small test data.
// -----------------------------------------------------------------------------

func TestStore_PutMultipartFromFile(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Create a small temp file (multipart works with any size when called directly)
		data := make([]byte, 10*1024*1024) // 10MB - enough for 2 parts
		for i := range data {
			data[i] = byte(i % 256)
		}

		tmpFile, err := os.CreateTemp(t.TempDir(), "test-*")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer closer(tmpFile)()

		if _, err := tmpFile.Write(data); err != nil {
			t.Fatalf("failed to write temp file: %v", err)
		}
		if _, err := tmpFile.Seek(0, 0); err != nil {
			t.Fatalf("failed to seek temp file: %v", err)
		}

		// Call internal multipart path directly
		err = store.putMultipartFromFile(ctx, "multipart-test.bin", tmpFile, int64(len(data)))
		if err != nil {
			t.Fatalf("putMultipartFromFile failed: %v", err)
		}

		// Verify multipart upload was used
		mock.mu.RLock()
		createCalls := mock.CreateMultipartUploadCalls
		putCalls := mock.PutObjectCalls
		stored := mock.objects["multipart-test.bin"]
		mock.mu.RUnlock()

		if createCalls != 1 {
			t.Errorf("expected 1 CreateMultipartUpload call, got %d", createCalls)
		}
		if putCalls != 0 {
			t.Errorf("expected 0 PutObject calls (multipart path), got %d", putCalls)
		}
		if !bytes.Equal(data, stored) {
			t.Error("stored data does not match original")
		}
	})

	t.Run("pre existing returns err path exists", func(t *testing.T) {
		// Test multipart preflight detection by calling internal method directly
		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Pre-create an object via atomic path
		data := []byte("existing data")
		err := store.Put(ctx, "existing.bin", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("first Put failed: %v", err)
		}

		// Now try multipart to same path - should fail at preflight
		tmpFile, _ := os.CreateTemp(t.TempDir(), "test-*")
		defer closer(tmpFile)()
		_, _ = tmpFile.Write([]byte("new data"))
		_, _ = tmpFile.Seek(0, 0)

		err = store.putMultipartFromFile(ctx, "existing.bin", tmpFile, 8)
		if !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("expected ErrPathExists from preflight check, got: %v", err)
		}

		// Verify no multipart upload was started (failed at preflight HeadObject)
		mock.mu.RLock()
		multipartCalls := mock.CreateMultipartUploadCalls
		mock.mu.RUnlock()

		if multipartCalls != 0 {
			t.Errorf("expected 0 CreateMultipartUpload calls (should fail at preflight), got %d", multipartCalls)
		}
	})

	t.Run("content integrity", func(t *testing.T) {
		// Test multipart content integrity by calling internal method directly
		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Create data with known pattern (6MB = 2 parts)
		size := 6 * 1024 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		tmpFile, _ := os.CreateTemp(t.TempDir(), "test-*")
		defer closer(tmpFile)()
		_, _ = tmpFile.Write(data)
		_, _ = tmpFile.Seek(0, 0)

		err := store.putMultipartFromFile(ctx, "large-file.bin", tmpFile, int64(size))
		if err != nil {
			t.Fatalf("putMultipartFromFile failed: %v", err)
		}

		// Verify the data was stored correctly
		rc, err := store.Get(ctx, "large-file.bin")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer closer(rc)()

		retrieved, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if len(retrieved) != size {
			t.Errorf("expected %d bytes, got %d", size, len(retrieved))
		}

		if !bytes.Equal(data, retrieved) {
			t.Error("retrieved data does not match original")
		}
	})

	t.Run("failure triggers abort", func(t *testing.T) {
		// Verify that when multipart upload fails, AbortMultipartUpload is called.
		// This tests the actual abort path by calling internal method directly.

		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Configure mock to fail on second part upload (first part succeeds, then fails)
		mock.UploadPartFailOnCall = 2

		// Create data that requires multiple parts (11MB = 3 parts with 5MB part size)
		size := 11 * 1024 * 1024
		data := make([]byte, size)

		tmpFile, _ := os.CreateTemp(t.TempDir(), "test-*")
		defer closer(tmpFile)()
		_, _ = tmpFile.Write(data)
		_, _ = tmpFile.Seek(0, 0)

		// Call multipart path directly - should fail due to simulated UploadPart failure
		err := store.putMultipartFromFile(ctx, "will-fail.bin", tmpFile, int64(size))
		if err == nil {
			t.Fatal("expected putMultipartFromFile to fail due to simulated UploadPart error")
		}

		// Verify abort was called
		mock.mu.RLock()
		abortCalls := mock.AbortMultipartUploadCalls
		createCalls := mock.CreateMultipartUploadCalls
		numUploads := len(mock.uploads)
		mock.mu.RUnlock()

		if createCalls != 1 {
			t.Errorf("expected 1 CreateMultipartUpload call, got %d", createCalls)
		}
		if abortCalls != 1 {
			t.Errorf("expected 1 AbortMultipartUpload call (cleanup), got %d", abortCalls)
		}
		if numUploads != 0 {
			t.Errorf("expected 0 in-progress uploads after abort, got %d", numUploads)
		}

		// Verify object was not created
		mock.mu.RLock()
		_, exists := mock.objects["will-fail.bin"]
		mock.mu.RUnlock()
		if exists {
			t.Error("object should not exist after failed upload")
		}
	})

	t.Run("multipart conditional completion", func(t *testing.T) {
		// Assert: maxAtomicPutSize is 5GB (the documented threshold)
		const expectedThreshold = 5 * 1024 * 1024 * 1024
		if maxAtomicPutSize != expectedThreshold {
			t.Fatalf("maxAtomicPutSize should be %d (5GB), got %d", expectedThreshold, maxAtomicPutSize)
		}

		// Assert: Atomic path uses If-None-Match (atomic)
		// Verified by TestStore_Put_Atomic_Duplicate_ReturnsErrPathExists

		// Assert: Multipart path uses preflight + If-None-Match on completion
		// Verified by TestStore_PutMultipartFromFile_PreExisting_ReturnsErrPathExists
		// and TestStore_PutMultipartFromFile_ConditionalCompletion_ReturnsErrPathExists

		t.Log("Both atomic and multipart paths now use If-None-Match for atomic no-overwrite.")
		t.Log("Preflight check is an optimization to fail fast before uploading parts.")
	})

	// TestStore_PutMultipartFromFile_ConditionalCompletion_ReturnsErrPathExists verifies
	// that even if preflight check passes, a concurrent write is detected at completion.
	// This uses a custom mock that injects the race condition deterministically.
	t.Run("conditional completion returns err path exists", func(t *testing.T) {
		ctx := t.Context()

		// Create a racing mock that injects object just before CompleteMultipartUpload is checked
		mock := &racingMockS3Client{MockS3Client: NewMockS3Client()}
		store, _ := New(mock, Config{Bucket: "test"})

		// Create temp file
		tmpFile, err := os.CreateTemp("", "multipart-cond-*")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Remove(tmpFile.Name()) }()
		defer closer(tmpFile)()

		// Write data
		data := []byte("test multipart conditional completion")
		if _, err := tmpFile.Write(data); err != nil {
			t.Fatal(err)
		}
		if _, err := tmpFile.Seek(0, 0); err != nil {
			t.Fatal(err)
		}

		const testKey = "conditional-race.txt"
		mock.raceKey = testKey

		// Call putMultipartFromFile - the racing mock will inject the object
		// after all parts are uploaded but before completion is checked
		err = store.putMultipartFromFile(ctx, testKey, tmpFile, int64(len(data)))

		if err == nil {
			t.Fatal("expected error when completing multipart upload on concurrently-created key")
		}

		// Should get ErrPathExists (translated from PreconditionFailed)
		if !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("expected ErrPathExists, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Put duplicate behavior tests (per CONTRACT_STORAGE.md)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Put multipart content integrity test
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Multipart abort cleanup test
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Temp file cleanup tests
//
// These tests use dependency injection via the createTemp field to track
// temp file creation and verify deterministic cleanup. This approach is
// platform-independent (no TMPDIR/TMP/TEMP variance).
// -----------------------------------------------------------------------------

// tempFileTracker creates a test-owned temp file factory that tracks created files.
// Safe for concurrent use.
type tempFileTracker struct {
	dir     string
	mu      sync.Mutex
	created []string
	count   atomic.Int32
}

// newTempFileTracker returns a tracker that creates temp files in dir.
func newTempFileTracker(dir string) *tempFileTracker {
	return &tempFileTracker{dir: dir}
}

// createTemp implements the temp file factory interface for Store.
func (tr *tempFileTracker) createTemp() (*os.File, error) {
	f, err := os.CreateTemp(tr.dir, "lode-s3-*")
	if err != nil {
		return nil, err
	}
	tr.mu.Lock()
	tr.created = append(tr.created, f.Name())
	tr.mu.Unlock()
	tr.count.Add(1)
	return f, nil
}

// assertAllCleaned verifies all tracked temp files have been removed.
func (tr *tempFileTracker) assertAllCleaned(t *testing.T) {
	t.Helper()
	for _, path := range tr.created {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("temp file leak: %s still exists", filepath.Base(path))
		}
	}
}

// -----------------------------------------------------------------------------
// S3 limit validation tests
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Multipart conditional completion test
//
// This test verifies that multipart uploads use conditional completion
// (If-None-Match) to provide atomic no-overwrite guarantees.
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Get tests
// -----------------------------------------------------------------------------

func TestStore_Get(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello world")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		rc, err := store.Get(ctx, "test.txt")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer closer(rc)()

		data, _ := io.ReadAll(rc)
		if string(data) != string(content) {
			t.Errorf("expected %q, got %q", string(content), string(data))
		}
	})

	t.Run("err not found", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.Get(ctx, "nonexistent.txt")
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("err invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.Get(ctx, "")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for empty path, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Exists tests
// -----------------------------------------------------------------------------

func TestStore_Exists(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("hello")))

		exists, err := store.Exists(ctx, "test.txt")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("expected exists=true")
		}
	})

	t.Run("false", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		exists, err := store.Exists(ctx, "nonexistent.txt")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("expected exists=false")
		}
	})

	t.Run("err invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.Exists(ctx, "")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Delete tests
// -----------------------------------------------------------------------------

func TestStore_Delete(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_ = store.Put(ctx, "test.txt", bytes.NewReader([]byte("hello")))

		err := store.Delete(ctx, "test.txt")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		exists, _ := store.Exists(ctx, "test.txt")
		if exists {
			t.Error("file should not exist after delete")
		}
	})

	t.Run("not exists idempotent", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		// Delete on non-existent file should not error (idempotent)
		err := store.Delete(ctx, "nonexistent.txt")
		if err != nil {
			t.Errorf("Delete on non-existent file should not error, got: %v", err)
		}
	})

	t.Run("err invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.Delete(ctx, "")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// List tests
// -----------------------------------------------------------------------------

func TestStore_List(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		keys, err := store.List(ctx, "")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("expected empty list, got %d keys", len(keys))
		}
	})

	t.Run("with prefix", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_ = store.Put(ctx, "a/1.txt", bytes.NewReader([]byte("1")))
		_ = store.Put(ctx, "a/2.txt", bytes.NewReader([]byte("2")))
		_ = store.Put(ctx, "b/3.txt", bytes.NewReader([]byte("3")))

		keys, err := store.List(ctx, "a/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
		}
	})

	// TestStore_List_StringPrefixSemantics verifies List is a string-prefix match
	// (CONTRACT_STORAGE §List): a trailing "/" is part of the prefix and excludes
	// siblings that share the bare name; a partial final token matches.
	t.Run("string prefix semantics", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_ = store.Put(ctx, "day=2024-01-15/1.txt", bytes.NewReader([]byte("1")))
		_ = store.Put(ctx, "day=2024-01-150/2.txt", bytes.NewReader([]byte("2")))
		_ = store.Put(ctx, "day=2024-02-01/3.txt", bytes.NewReader([]byte("3")))

		cases := []struct {
			prefix string
			want   int
		}{
			{"day=2024-01-15/", 1}, // trailing slash excludes the "-150" sibling
			{"day=2024-01-15", 2},  // bare string prefix includes it
			{"day=2024-01", 2},     // partial final token
			{"day=2030", 0},
			{"/", 3}, // slash-only prefix is the store root, not the key "/"
			{"", 3},
		}
		for _, tc := range cases {
			keys, err := store.List(ctx, tc.prefix)
			if err != nil {
				t.Fatalf("List(%q): %v", tc.prefix, err)
			}
			if len(keys) != tc.want {
				t.Errorf("List(%q) = %v, want %d keys", tc.prefix, keys, tc.want)
			}
		}
	})

	t.Run("with store prefix", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test", Prefix: "datasets/"})

		_ = store.Put(ctx, "foo/1.txt", bytes.NewReader([]byte("1")))
		_ = store.Put(ctx, "foo/2.txt", bytes.NewReader([]byte("2")))

		keys, err := store.List(ctx, "foo/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
		}

		// Keys should be relative (without store prefix)
		for _, key := range keys {
			if !slices.Contains([]string{"foo/1.txt", "foo/2.txt"}, key) {
				t.Errorf("unexpected key: %s", key)
			}
		}
	})

	t.Run("err invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.List(ctx, "../")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// ReadRange tests
// -----------------------------------------------------------------------------

func TestStore_ReadRange(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello world")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		// Read "world"
		data, err := store.ReadRange(ctx, "test.txt", 6, 5)
		if err != nil {
			t.Fatalf("ReadRange failed: %v", err)
		}
		if string(data) != "world" {
			t.Errorf("expected 'world', got %q", string(data))
		}
	})

	t.Run("beyond EOF", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		// Read beyond EOF - should return available bytes
		data, err := store.ReadRange(ctx, "test.txt", 3, 100)
		if err != nil {
			t.Fatalf("ReadRange failed: %v", err)
		}
		if string(data) != "lo" {
			t.Errorf("expected 'lo', got %q", string(data))
		}
	})

	t.Run("offset beyond EOF", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		// Offset beyond EOF - should return empty slice
		data, err := store.ReadRange(ctx, "test.txt", 100, 10)
		if err != nil {
			t.Fatalf("ReadRange failed: %v", err)
		}
		if len(data) != 0 {
			t.Errorf("expected empty slice, got %d bytes", len(data))
		}
	})

	t.Run("not found", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 10)
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("zero length", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello world")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		// Zero-length read should return empty slice without error
		data, err := store.ReadRange(ctx, "test.txt", 5, 0)
		if err != nil {
			t.Fatalf("ReadRange with length=0 failed: %v", err)
		}
		if len(data) != 0 {
			t.Errorf("expected empty slice for length=0, got %d bytes", len(data))
		}
	})

	t.Run("zero length not found", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		// Zero-length read on non-existent file must return ErrNotFound per contract
		_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 0)
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound for zero-length read on missing file, got: %v", err)
		}
	})
}

func TestStore_ReadRange_Bounds(t *testing.T) {
	t.Run("negative offset", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReadRange(ctx, "test.txt", -1, 10)
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
		}
	})

	t.Run("negative length", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReadRange(ctx, "test.txt", 0, -1)
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for negative length, got: %v", err)
		}
	})

	t.Run("length overflow", func(t *testing.T) {
		// This test validates 32-bit platform protection.
		if math.MaxInt == math.MaxInt64 {
			t.Skip("length overflow check only applies to 32-bit platforms")
		}

		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
		}
	})

	t.Run("offset plus length overflow", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		// offset + length would overflow int64
		_, err := store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// ReaderAt tests
// -----------------------------------------------------------------------------

func TestStore_ReaderAt(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello world")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		ra, err := store.ReaderAt(ctx, "test.txt")
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}

		buf := make([]byte, 5)
		n, err := ra.ReadAt(buf, 6)
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}
		if n != 5 || string(buf) != "world" {
			t.Errorf("expected 'world', got %q", string(buf[:n]))
		}
	})

	t.Run("concurrent reads", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("0123456789")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		ra, err := store.ReaderAt(ctx, "test.txt")
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}

		// Read different offsets concurrently
		done := make(chan bool, 2)

		go func() {
			buf := make([]byte, 3)
			n, err := ra.ReadAt(buf, 0)
			if err != nil || n != 3 || string(buf) != "012" {
				t.Errorf("read at 0: expected '012', got %q (err=%v)", string(buf[:n]), err)
			}
			done <- true
		}()

		go func() {
			buf := make([]byte, 3)
			n, err := ra.ReadAt(buf, 7)
			if err != nil || n != 3 || string(buf) != "789" {
				t.Errorf("read at 7: expected '789', got %q (err=%v)", string(buf[:n]), err)
			}
			done <- true
		}()

		<-done
		<-done
	})

	t.Run("not found", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReaderAt(ctx, "nonexistent.txt")
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("err invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		_, err := store.ReaderAt(ctx, "")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath, got: %v", err)
		}
	})

	t.Run("EOF", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		content := []byte("hello")
		_ = store.Put(ctx, "test.txt", bytes.NewReader(content))

		ra, err := store.ReaderAt(ctx, "test.txt")
		if err != nil {
			t.Fatalf("ReaderAt failed: %v", err)
		}

		// Read beyond EOF
		buf := make([]byte, 10)
		_, err = ra.ReadAt(buf, 100)
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got: %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Test Mocks
// -----------------------------------------------------------------------------

// racingMockS3Client wraps MockS3Client and injects a race condition in
// CompleteMultipartUpload to test conditional completion guarantees.
type racingMockS3Client struct {
	*MockS3Client
	raceKey string // key to inject race condition for
}

// CompleteMultipartUpload injects a race condition before calling the parent.
// This simulates another writer creating the object between UploadPart and completion.
func (m *racingMockS3Client) CompleteMultipartUpload(ctx context.Context, params *s3api.CompleteMultipartUploadInput, opts ...func(*s3api.Options)) (*s3api.CompleteMultipartUploadOutput, error) {
	// Inject race condition: create the object just before completion check
	if m.raceKey != "" && aws.ToString(params.Key) == m.raceKey {
		m.mu.Lock()
		m.objects[m.raceKey] = []byte("race winner's data")
		m.mu.Unlock()
	}
	return m.MockS3Client.CompleteMultipartUpload(ctx, params, opts...)
}

// -----------------------------------------------------------------------------
// CompareAndSwap tests — S3 store
// -----------------------------------------------------------------------------

func TestStore_CompareAndSwap(t *testing.T) {
	t.Run("create when empty", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1")
		if err != nil {
			t.Fatalf("CompareAndSwap create failed: %v", err)
		}

		rc, err := store.Get(ctx, "ptr/latest")
		if err != nil {
			t.Fatalf("Get after CAS create failed: %v", err)
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		if string(data) != "snap-1" {
			t.Errorf("expected 'snap-1', got %q", string(data))
		}
	})

	t.Run("update match", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("initial create failed: %v", err)
		}

		if err := store.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2"); err != nil {
			t.Fatalf("CompareAndSwap update failed: %v", err)
		}

		rc, err := store.Get(ctx, "ptr/latest")
		if err != nil {
			t.Fatalf("Get after CAS update: %v", err)
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		if string(data) != "snap-2" {
			t.Errorf("expected 'snap-2', got %q", string(data))
		}
	})

	t.Run("conflict mismatch", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("initial create failed: %v", err)
		}

		err := store.CompareAndSwap(ctx, "ptr/latest", "stale", "snap-2")
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Errorf("expected ErrSnapshotConflict, got: %v", err)
		}
	})

	t.Run("conflict not exist", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.CompareAndSwap(ctx, "ptr/latest", "snap-0", "snap-1")
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Errorf("expected ErrSnapshotConflict, got: %v", err)
		}
	})

	t.Run("if match used", func(t *testing.T) {
		ctx := t.Context()
		mock := NewMockS3Client()
		store, _ := New(mock, Config{Bucket: "test"})

		// Create initial content.
		if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("initial create failed: %v", err)
		}

		// The first Put uses If-None-Match. Reset counter.
		mock.ResetCounts()

		// Update with matching content.
		if err := store.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2"); err != nil {
			t.Fatalf("CompareAndSwap update failed: %v", err)
		}

		// Should have called PutObject once (with If-Match).
		if mock.PutObjectCalls != 1 {
			t.Errorf("expected 1 PutObject call for update, got %d", mock.PutObjectCalls)
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		ctx := t.Context()
		store, _ := New(NewMockS3Client(), Config{Bucket: "test"})

		err := store.CompareAndSwap(ctx, "", "", "snap-1")
		if !errors.Is(err, lode.ErrInvalidPath) {
			t.Errorf("expected ErrInvalidPath for empty key, got: %v", err)
		}
	})
}

// Compile-time interface assertion.
var _ lode.ConditionalWriter = (*Store)(nil)
