package lode

import (
	"errors"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// Every caller-supplied value Lode refuses is reported as ErrInvalidArgument
// (CONTRACT_ERRORS.md §Configuration Errors), so callers classify with
// errors.Is instead of matching message text. Each case names the value in
// its message. Volume's cases live in package lode/volume.
func TestErrInvalidArgument_Classification(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	factory := Use(NewMemory())
	nilStore := Factory[Store](func() (Store, error) { return nil, nil }) //nolint:nilnil // the misbehaving factory under test

	records, err := Open(factory).Records(ctx, "records", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := Open(factory).Blobs(ctx, "blobs")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		call func() error
		want string // substring naming the value
	}{
		{"Open nil factory", func() error { _, err := Open[Store](nil).Blobs(ctx, "d"); return err }, "store factory"},
		{"Open nil store", func() error { _, err := Open(nilStore).Blobs(ctx, "d"); return err }, "nil store"},
		{"OpenReadOnly nil factory", func() error { _, err := OpenReadOnly[Store](nil).Blobs(ctx, "d"); return err }, "store factory"},
		{"OpenReadOnly nil store", func() error { _, err := OpenReadOnly(nilStore).Blobs(ctx, "d"); return err }, "nil store"},
		{"root option nil", func() error { _, err := Open(factory, nil).Blobs(ctx, "d"); return err }, "root option"},
		{"FS option nil", func() error { _, err := Open(FS(t.TempDir(), nil)).Blobs(ctx, "d"); return err }, "fs option"},
		{"dataset ID", func() error { _, err := Open(factory).Blobs(ctx, "a/b"); return err }, "dataset ID"},
		{"view dataset ID", func() error { _, err := OpenReadOnly(factory).Blobs(ctx, "a/b"); return err }, "dataset ID"},
		{"RemoveDataset dataset ID", func() error { return Open(factory).RemoveDataset(ctx, "a/b") }, "dataset ID"},
		{"RemoveDataset nil option", func() error { return Open(factory).RemoveDataset(ctx, "d", nil) }, "option"},
		{"WithAnchor snapshot ID", func() error { _, err := OpenReadOnly(factory).Blobs(ctx, "d", WithAnchor("a/b")); return err }, "WithAnchor"},
		{"nil codec", func() error { _, err := Open(factory).Records(ctx, "d", Codec[D](nil)); return err }, "codec"},
		{"nil option", func() error { _, err := Open(factory).Blobs(ctx, "d", nil); return err }, "option"},
		{"nil view option", func() error { _, err := OpenReadOnly(factory).Blobs(ctx, "d", nil); return err }, "option"},
		{"nil layout", func() error { _, err := Open(factory).Blobs(ctx, "d", WithLayout(nil)); return err }, "layout"},
		{"nil compressor", func() error { _, err := Open(factory).Blobs(ctx, "d", WithCompressor(nil)); return err }, "compressor"},
		{"partitioned blobs", func() error { _, err := Open(factory).Blobs(ctx, "d", hive("day")); return err }, "partitioned"},
		{"Hive without keys", func() error { _, err := Open(factory).Records(ctx, "d", JSONL[D](), hive()); return err }, "partition key"},
		{"Hive key with =", func() error { _, err := Open(factory).Records(ctx, "d", JSONL[D](), hive("a=b")); return err }, "partition key"},
		{"Hive without By for typed records", func() error {
			_, err := Open(factory).Records(ctx, "d", JSONL[jsonlEvent](), hive("source"))
			return err
		}, "requires By"},
		{"WithRetry negative", func() error { _, err := Open(factory).Blobs(ctx, "d", WithRetry(-1)); return err }, "WithRetry"},
		{"WithRetryBaseDelay zero", func() error { _, err := Open(factory).Blobs(ctx, "d", WithRetryBaseDelay(0)); return err }, "WithRetryBaseDelay"},
		{"WithRetryMaxDelay zero", func() error { _, err := Open(factory).Blobs(ctx, "d", WithRetryMaxDelay(0)); return err }, "WithRetryMaxDelay"},
		{"WithRetryJitter out of range", func() error { _, err := Open(factory).Blobs(ctx, "d", WithRetryJitter(2)); return err }, "WithRetryJitter"},
		{"WithRetryObserver nil", func() error { _, err := Open(factory).Blobs(ctx, "d", WithRetryObserver(nil)); return err }, "WithRetryObserver"},
		{"Records.StreamWrite nil iterator", func() error { _, err := records.StreamWrite(ctx, nil, nil); return err }, "records iterator"},
		{"Blobs.StreamWrite nil reader", func() error { _, err := blobs.StreamWrite(ctx, nil, nil); return err }, "blob reader"},
		{"metadata that is not an object", func() error { _, err := records.Write(ctx, R(D{"a": 1}), []int{1}); return err }, "metadata"},
		{"metadata that does not encode", func() error { _, err := records.Write(ctx, R(D{"a": 1}), make(chan int)); return err }, "metadata"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("message %q does not name the value (%q)", err, c.want)
			}
			if !strings.HasPrefix(err.Error(), "lode: ") || strings.Contains(err.Error(), "lode: lode:") {
				t.Errorf("message %q must carry exactly one lode: prefix", err)
			}
		})
	}
}

// The sentinels in api.go keep their identity under errors.Is and their messages.
func TestSentinels_Messages(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want string
	}{
		{ErrInvalidArgument, "invalid argument"},
		{ErrInvalidPath, "invalid path"},
		{ErrManifestInvalid, "invalid manifest"},
		{ErrSnapshotExpired, "snapshot expired"},
		{ErrRemoved, "removed"},
		{ErrHeadUncommitted, "head snapshot is not committed yet"},
		{ErrSettingsMismatch, "settings mismatch"},
		{ErrChecksumMismatch, "checksum mismatch"},
		{ErrNotPartitioned, "operation requires a partitioned layout"},
		{ErrReadOnly, "read-only store"},
	}
	for _, c := range cases {
		if c.err.Error() != c.want {
			t.Errorf("%T: got %q, want %q", c.err, c.err.Error(), c.want)
		}
	}
}

// Argument errors come before any store work: an invalid root option is
// reported without calling the factory, and an invalid dataset option or
// metadata value without any store call.
func TestErrInvalidArgument_BeforeStoreWork(t *testing.T) {
	t.Parallel()
	t.Run("root options before the factory", func(t *testing.T) {
		calls := 0
		failing := Factory[Store](func() (Store, error) { calls++; return nil, errors.New("factory down") })
		if _, err := Open(failing, nil).Blobs(t.Context(), "d"); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
		if calls != 0 {
			t.Errorf("store factory called %d times before argument validation", calls)
		}
	})

	t.Run("dataset arguments before any store call", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		cases := map[string]func() error{
			"option":     func() error { _, err := Open(Use(fs)).Blobs(t.Context(), "d", WithRetry(-1)); return err },
			"dataset ID": func() error { _, err := Open(Use(fs)).Records(t.Context(), "a/../b", JSONL[D]()); return err },
			"view":       func() error { _, err := OpenReadOnly(Use(fs)).Blobs(t.Context(), "", nil); return err },
			"remove":     func() error { return Open(Use(fs)).RemoveDataset(t.Context(), "../b") },
		}
		for name, call := range cases {
			fs.Reset()
			if err := call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("%s: got %v, want ErrInvalidArgument", name, err)
			}
			if n := len(fs.GetCalls()) + len(fs.ListCalls()) + len(fs.PutCalls()); n != 0 {
				t.Errorf("%s: %d store calls before argument validation", name, n)
			}
		}
	})

	t.Run("metadata before any store call", func(t *testing.T) {
		fs := testutil.NewFaultStore(NewMemory())
		ds, err := Open(Use(fs)).Records(t.Context(), "d", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		fs.Reset()
		if _, err := ds.Write(t.Context(), R(D{"a": 1}), "not an object"); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
		if n := len(fs.PutCalls()); n != 0 {
			t.Errorf("%d Puts before the metadata was validated", n)
		}
	})
}

// The sentinels declared with explicit `error` type keep the static type
// `error`, so downstream code such as `err := lode.ErrInvalidPath` followed by
// another assignment keeps compiling.
func TestMovedSentinels_StaticTypeIsError(t *testing.T) {
	t.Parallel()
	for name, ptr := range map[string]any{
		"ErrInvalidArgument":  &ErrInvalidArgument,
		"ErrInvalidPath":      &ErrInvalidPath,
		"ErrManifestInvalid":  &ErrManifestInvalid,
		"ErrSettingsMismatch": &ErrSettingsMismatch,
		"ErrChecksumMismatch": &ErrChecksumMismatch,
		"ErrRemoved":          &ErrRemoved,
		"ErrReadOnly":         &ErrReadOnly,
	} {
		if _, ok := ptr.(*error); !ok {
			t.Errorf("%s is declared as %T, want *error", name, ptr)
		}
	}
}
