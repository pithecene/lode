package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/lode/lode"
)

// lostResponseClient stores the object a PutObject carries and then answers
// as a backend does when a transport retries that request: the first
// attempt was stored, the retry meets it, and the conditional write is
// refused.
type lostResponseClient struct {
	*MockS3Client
	code    string
	headErr error
}

func (c *lostResponseClient) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	out, err := c.MockS3Client.PutObject(ctx, params, optFns...)
	if err != nil {
		return out, err
	}
	code := c.code
	if code == "" {
		code = "PreconditionFailed"
	}
	return nil, &smithyAPIError{code: code, message: "object already exists"}
}

func (c *lostResponseClient) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if c.headErr != nil {
		return nil, c.headErr
	}
	return c.MockS3Client.HeadObject(ctx, params, optFns...)
}

// A refused conditional write whose object is the call's own payload is a
// success (CONTRACT_STORAGE §Put).
func TestStore_Put_OwnUploadAfterLostResponse(t *testing.T) {
	ctx := t.Context()
	mock := &lostResponseClient{MockS3Client: NewMockS3Client()}
	store, _ := New(mock, "test")

	payload := []byte("stored once, answered twice")
	if err := store.Put(ctx, "obj", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put whose retry met its own object: got %v, want nil", err)
	}
	rc, err := store.Get(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, payload) {
		t.Fatalf("stored %q, want %q", got, payload)
	}
}

// A refused write whose object holds other bytes belongs to another writer.
func TestStore_Put_OtherWritersObjectStaysRefused(t *testing.T) {
	ctx := t.Context()
	mock := NewMockS3Client()
	store, _ := New(mock, "test")

	if err := store.Put(ctx, "obj", bytes.NewReader([]byte("theirs"))); err != nil {
		t.Fatal(err)
	}
	err := store.Put(ctx, "obj", bytes.NewReader([]byte("ours, same length"[:6])))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("Put over another writer's object: got %v, want ErrPathExists", err)
	}
}

// A refusal the adapter cannot confirm as its own, because the HEAD fails,
// stays a refusal.
func TestStore_Put_UnconfirmedRefusalStaysRefused(t *testing.T) {
	ctx := t.Context()
	mock := &lostResponseClient{MockS3Client: NewMockS3Client(), headErr: errors.New("head unavailable")}
	store, _ := New(mock, "test")

	err := store.Put(ctx, "obj", bytes.NewReader([]byte("unconfirmed")))
	if !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("Put whose HEAD failed: got %v, want ErrPathExists", err)
	}
}

// The adapter compares the payload's MD5 with the ETag, so a multipart
// ETag, which carries a part count, never passes as the adapter's own.
func TestStore_Put_MultipartETagNeverMatches(t *testing.T) {
	ctx := t.Context()
	mock := &lostResponseClient{MockS3Client: NewMockS3Client()}
	store, _ := New(mock, "test")
	mock.mu.Lock()
	mock.objects["obj"] = []byte("x")
	mock.mu.Unlock()
	// The mock's HeadObject reports the stored object, so this case asks the
	// comparison directly with a multipart ETag.
	if store.ownUpload(ctx, "obj", 1, "9dd4e461268c8034f5c8564e155c67a6-2") {
		t.Fatal("a multipart ETag passed as the adapter's own upload")
	}
	if !store.ownUpload(ctx, "obj", 1, "9dd4e461268c8034f5c8564e155c67a6") {
		t.Fatal("the stored object's own MD5 did not pass")
	}
	_ = aws.String
}

// AWS S3 returns 409 ConditionalRequestConflict when concurrent conditional
// writes race on the same key; Put maps it just like 412 PreconditionFailed.
func TestStore_Put_ConditionalRequestConflictMapped(t *testing.T) {
	ctx := t.Context()
	for _, code := range []string{"ConditionalRequestConflict", "409"} {
		t.Run(code, func(t *testing.T) {
			own := &lostResponseClient{MockS3Client: NewMockS3Client(), code: code}
			ownStore, _ := New(own, "test")
			if err := ownStore.Put(ctx, "own", bytes.NewReader([]byte("mine"))); err != nil {
				t.Fatalf("own upload after %s: got %v, want nil", code, err)
			}

			other := &lostResponseClient{MockS3Client: NewMockS3Client(), code: code, headErr: errors.New("other writer in flight")}
			otherStore, _ := New(other, "test")
			if err := otherStore.Put(ctx, "other", bytes.NewReader([]byte("theirs"))); !errors.Is(err, lode.ErrPathExists) {
				t.Fatalf("other writer %s: got %v, want ErrPathExists", code, err)
			}
		})
	}
}
