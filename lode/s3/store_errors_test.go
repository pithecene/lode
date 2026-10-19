package s3

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3api "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene-io/lode/lode"
)

// Error-code mapping and exact range-header tests. The mock client only ever
// returns "PreconditionFailed", so the alternate S3 spellings ("412",
// "ConditionalRequestConflict", "409") and the whole of mapCASError were
// unobserved, and no test asserted the Range header a read produces.
//
// Known equivalent or untestable mutants left alive in s3/store.go: the 5TB
// object-size and 50GB part-scaling boundaries (would require uploading that
// much data); the abort-context timeout constant; `remaining < thisPartSize`
// → `<=` (an equal final part uploads the same bytes).

// codeClient wraps the mock to fail chosen operations with a given S3 error
// code and to record the last Range header seen by GetObject.
type codeClient struct {
	*MockS3Client
	mu           sync.Mutex
	putCode      string
	completeCode string
	lastRange    string
}

func apiErr(code string) error { return &smithyAPIError{code: code, message: code} }

func (c *codeClient) PutObject(ctx context.Context, p *s3api.PutObjectInput, o ...func(*s3api.Options)) (*s3api.PutObjectOutput, error) {
	c.mu.Lock()
	code := c.putCode
	c.mu.Unlock()
	if code != "" {
		return nil, apiErr(code)
	}
	return c.MockS3Client.PutObject(ctx, p, o...)
}

func (c *codeClient) CompleteMultipartUpload(ctx context.Context, p *s3api.CompleteMultipartUploadInput, o ...func(*s3api.Options)) (*s3api.CompleteMultipartUploadOutput, error) {
	c.mu.Lock()
	code := c.completeCode
	c.mu.Unlock()
	if code != "" {
		return nil, apiErr(code)
	}
	return c.MockS3Client.CompleteMultipartUpload(ctx, p, o...)
}

func (c *codeClient) GetObject(ctx context.Context, p *s3api.GetObjectInput, o ...func(*s3api.Options)) (*s3api.GetObjectOutput, error) {
	c.mu.Lock()
	c.lastRange = aws.ToString(p.Range)
	c.mu.Unlock()
	return c.MockS3Client.GetObject(ctx, p, o...)
}

func (c *codeClient) setPutCode(code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putCode = code
}

func (c *codeClient) rangeSeen() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRange
}

var conditionalCodes = []string{"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}

func TestStore_Put_MapsPreconditionCodes(t *testing.T) {
	ctx := t.Context()
	for _, code := range []string{"PreconditionFailed", "412"} {
		client := &codeClient{MockS3Client: NewMockS3Client(), putCode: code}
		store, _ := New(client, Config{Bucket: "test"})
		if err := store.Put(ctx, "k", bytes.NewReader([]byte("x"))); !errors.Is(err, lode.ErrPathExists) {
			t.Errorf("code %s: got %v, want ErrPathExists", code, err)
		}
	}
	// Atomic PutObject only treats the 412 spellings as "exists"; a bare
	// conflict code is surfaced as a plain error.
	client := &codeClient{MockS3Client: NewMockS3Client(), putCode: "409"}
	store, _ := New(client, Config{Bucket: "test"})
	if err := store.Put(ctx, "k", bytes.NewReader([]byte("x"))); err == nil || errors.Is(err, lode.ErrPathExists) {
		t.Errorf("code 409: got %v, want a non-ErrPathExists error", err)
	}
}

func TestStore_PutMultipartFromFile_MapsCompletionCodes(t *testing.T) {
	ctx := t.Context()
	data := make([]byte, 6*1024*1024) // two parts
	for _, code := range append(conditionalCodes, "SlowDown") {
		client := &codeClient{MockS3Client: NewMockS3Client(), completeCode: code}
		store, _ := New(client, Config{Bucket: "test"})
		tmp, err := os.CreateTemp(t.TempDir(), "mp-*")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = tmp.Write(data)
		err = store.putMultipartFromFile(ctx, "large.bin", tmp, int64(len(data)))
		_ = tmp.Close()
		wantExists := code != "SlowDown"
		if errors.Is(err, lode.ErrPathExists) != wantExists || err == nil {
			t.Errorf("code %s: got %v, want ErrPathExists=%v", code, err, wantExists)
		}
	}
}

func TestStore_CompareAndSwap_MapsPreconditionCodes(t *testing.T) {
	ctx := t.Context()
	for _, code := range append(conditionalCodes, "SlowDown") {
		wantConflict := code != "SlowDown"

		// Create path (object absent, If-None-Match).
		client := &codeClient{MockS3Client: NewMockS3Client(), putCode: code}
		store, _ := New(client, Config{Bucket: "test"})
		err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1")
		if errors.Is(err, lode.ErrSnapshotConflict) != wantConflict || err == nil {
			t.Errorf("create, code %s: got %v, want conflict=%v", code, err, wantConflict)
		}

		// Update path (object present, If-Match).
		client = &codeClient{MockS3Client: NewMockS3Client()}
		store, _ = New(client, Config{Bucket: "test"})
		if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatal(err)
		}
		client.setPutCode(code)
		err = store.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2")
		if errors.Is(err, lode.ErrSnapshotConflict) != wantConflict || err == nil {
			t.Errorf("update, code %s: got %v, want conflict=%v", code, err, wantConflict)
		}
	}
}

func TestStore_ReadRange_ExactRangeHeader(t *testing.T) {
	ctx := t.Context()
	client := &codeClient{MockS3Client: NewMockS3Client()}
	store, _ := New(client, Config{Bucket: "test"})
	if err := store.Put(ctx, "k", bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadRange(ctx, "k", 2, 3)
	if err != nil || string(got) != "llo" {
		t.Fatalf("ReadRange(2,3) = %q, %v; want \"llo\"", got, err)
	}
	if r := client.rangeSeen(); r != "bytes=2-4" {
		t.Fatalf("ReadRange header = %q, want bytes=2-4", r)
	}

	ra, err := store.ReaderAt(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	n, err := ra.ReadAt(buf, 2)
	if err != nil || n != 3 || string(buf) != "llo" {
		t.Fatalf("ReadAt(3 bytes @2) = %d, %q, %v; want 3, \"llo\"", n, buf, err)
	}
	if r := client.rangeSeen(); r != "bytes=2-4" {
		t.Fatalf("ReadAt header = %q, want bytes=2-4", r)
	}
}

func TestStore_ReadRange_ExactOverflowBound(t *testing.T) {
	ctx := t.Context()
	store, _ := New(NewMockS3Client(), Config{Bucket: "test"})
	if err := store.Put(ctx, "k", bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	// Zero length is valid and answered without a request.
	got, err := store.ReadRange(ctx, "k", 1, 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("zero length: got %d bytes, %v; want empty, nil", len(got), err)
	}
	got, err = store.ReadRange(ctx, "k", math.MaxInt64-10, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("exact bound: got %d bytes, %v; want empty, nil", len(got), err)
	}
	if _, err := store.ReadRange(ctx, "k", math.MaxInt64-10, 11); !errors.Is(err, lode.ErrInvalidPath) {
		t.Fatalf("one past bound: got %v, want ErrInvalidPath", err)
	}
}
