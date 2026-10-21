package lode

import (
	"errors"
	"strings"
	"testing"
)

// Every caller-supplied value Lode refuses is reported as ErrInvalidArgument
// (CONTRACT_ERRORS.md §Configuration Errors), so callers classify with
// errors.Is instead of matching message text. Each case names the value in
// its message. Volume's cases live in package lode/volume.
func TestErrInvalidArgument_Classification(t *testing.T) {
	ctx := t.Context()
	factory := NewMemoryFactoryFrom(NewMemory())
	nilStore := func() (Store, error) { return nil, nil } //nolint:nilnil // the misbehaving factory under test

	blobDataset, err := NewDataset("blob", factory)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		call func() error
		want string // substring naming the value
	}{
		{"NewDataset nil factory", func() error { _, err := NewDataset("d", nil); return err }, "store factory"},
		{"NewDataset nil store", func() error { _, err := NewDataset("d", nilStore); return err }, "nil store"},
		{"NewDataset nil layout", func() error { _, err := NewDataset("d", factory, WithLayout(nil)); return err }, "layout"},
		{"NewDataset nil compressor", func() error { _, err := NewDataset("d", factory, WithCompressor(nil)); return err }, "compressor"},
		{"NewDataset blob mode partitioned", func() error { _, err := NewDataset("d", factory, WithHiveLayout("day")); return err }, "noop partitioner"},
		{"WithHiveLayout no keys", func() error {
			_, err := NewDataset("d", factory, WithCodec(NewJSONLCodec()), WithHiveLayout())
			return err
		}, "partition key"},
		{"NewHiveLayout no keys", func() error { _, err := NewHiveLayout(); return err }, "partition key"},
		{"WithRetryCount negative", func() error { _, err := NewDataset("d", factory, WithRetryCount(-1)); return err }, "WithRetryCount"},
		{"WithRetryBaseDelay zero", func() error { _, err := NewDataset("d", factory, WithRetryBaseDelay(0)); return err }, "WithRetryBaseDelay"},
		{"WithRetryMaxDelay zero", func() error { _, err := NewDataset("d", factory, WithRetryMaxDelay(0)); return err }, "WithRetryMaxDelay"},
		{"WithRetryJitter out of range", func() error { _, err := NewDataset("d", factory, WithRetryJitter(2)); return err }, "WithRetryJitter"},
		{"WithRetryObserver nil", func() error { _, err := NewDataset("d", factory, WithRetryObserver(nil)); return err }, "WithRetryObserver"},
		{"NewDatasetReader nil factory", func() error { _, err := NewDatasetReader(nil); return err }, "store factory"},
		{"NewDatasetReader nil store", func() error { _, err := NewDatasetReader(nilStore); return err }, "nil store"},
		{"NewDatasetReader nil layout", func() error { _, err := NewDatasetReader(factory, WithLayout(nil)); return err }, "layout"},
		{"Write blob count", func() error { _, err := blobDataset.Write(ctx, []any{}, nil); return err }, "exactly one"},
		{"Write blob type", func() error { _, err := blobDataset.Write(ctx, []any{"text"}, nil); return err }, "[]byte"},
		{"StreamWriteRecords without codec", func() error {
			_, err := blobDataset.StreamWriteRecords(ctx, &sliceIterator{}, nil)
			return err
		}, "requires a codec"},
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

// The sentinels that moved into api.go keep their identity under errors.Is
// and their messages.
func TestSentinels_Messages(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{ErrInvalidArgument, "invalid argument"},
		{ErrInvalidPath, "invalid path"},
		{ErrManifestInvalid, "invalid manifest"},
		{ErrOptionNotValidForDataset, "option not valid for dataset"},
		{ErrOptionNotValidForDatasetReader, "option not valid for reader"},
		{ErrSnapshotExpired, "snapshot expired"},
		{ErrHeadUncommitted, "head snapshot is not committed yet"},
	}
	for _, c := range cases {
		if c.err.Error() != c.want {
			t.Errorf("%T: got %q, want %q", c.err, c.err.Error(), c.want)
		}
	}
}

// Option errors carry the lode: prefix themselves; constructors must not add
// a second one.
func TestOptionErrors_SinglePrefix(t *testing.T) {
	factory := NewMemoryFactoryFrom(NewMemory())
	_, err := NewDatasetReader(factory, WithCodec(NewJSONLCodec()))
	if !errors.Is(err, ErrOptionNotValidForDatasetReader) {
		t.Fatalf("got %v, want ErrOptionNotValidForDatasetReader", err)
	}
	if got, want := err.Error(), "lode: WithCodec: option not valid for reader"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Argument errors come before any store work: an invalid option is reported
// even when the factory would fail, and the factory is not called.
func TestErrInvalidArgument_BeforeStoreFactory(t *testing.T) {
	calls := 0
	failing := func() (Store, error) { calls++; return nil, errors.New("factory down") } //nolint:unparam // a factory that always fails is the point

	cases := []struct {
		name string
		call func() error
	}{
		{"NewDataset", func() error { _, err := NewDataset("d", failing, WithRetryCount(-1)); return err }},
		{"NewDatasetReader", func() error { _, err := NewDatasetReader(failing, WithLayout(nil)); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			calls = 0
			if err := c.call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
			if calls != 0 {
				t.Errorf("store factory called %d times before argument validation", calls)
			}
		})
	}
}

// The sentinels that moved from errors.New values keep the static type
// `error`, so downstream code such as `err := lode.ErrInvalidPath` followed by
// another assignment keeps compiling. ErrInvalidArgument is declared the same
// way for consistency.
func TestMovedSentinels_StaticTypeIsError(t *testing.T) {
	for name, ptr := range map[string]any{
		"ErrInvalidArgument":                &ErrInvalidArgument,
		"ErrInvalidPath":                    &ErrInvalidPath,
		"ErrManifestInvalid":                &ErrManifestInvalid,
		"ErrOptionNotValidForDataset":       &ErrOptionNotValidForDataset,
		"ErrOptionNotValidForDatasetReader": &ErrOptionNotValidForDatasetReader,
	} {
		if _, ok := ptr.(*error); !ok {
			t.Errorf("%s is declared as %T, want *error", name, ptr)
		}
	}
}
