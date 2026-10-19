package s3

import (
	"bytes"
	"errors"
	"testing"
)

var errReaderBoom = errors.New("reader failure")

// failingReader yields prefix, then fails.
type failingReader struct {
	prefix *bytes.Reader
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.prefix.Len() > 0 {
		return f.prefix.Read(p)
	}
	return 0, errReaderBoom
}

// Put spools the reader before any upload, so a failing reader never creates
// an object (CONTRACT_STORAGE §Put: all-or-nothing).
func TestStore_Put_AllOrNothing(t *testing.T) {
	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, Config{Bucket: "test"})

	err := store.Put(ctx, "obj", &failingReader{prefix: bytes.NewReader([]byte("partial"))})
	if !errors.Is(err, errReaderBoom) {
		t.Fatalf("Put with failing reader: got %v, want wrapped reader error", err)
	}
	if mock.PutObjectCalls != 0 || mock.CreateMultipartUploadCalls != 0 {
		t.Fatalf("failed Put reached the backend: PutObject=%d multipart=%d", mock.PutObjectCalls, mock.CreateMultipartUploadCalls)
	}
	exists, err := store.Exists(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed Put left an object behind")
	}
	if err := store.Put(ctx, "obj", bytes.NewReader([]byte("complete"))); err != nil {
		t.Fatalf("Put after failed Put: %v", err)
	}
}
