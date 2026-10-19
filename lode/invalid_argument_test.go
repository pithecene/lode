package lode

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// Every caller-supplied value Lode refuses is reported as ErrInvalidArgument
// (CONTRACT_ERRORS.md §Configuration Errors), so callers classify with
// errors.Is instead of matching message text. Each case names the value in
// its message.
func TestErrInvalidArgument_Classification(t *testing.T) {
	ctx := t.Context()
	factory := NewMemoryFactoryFrom(NewMemory())
	nilStore := func() (Store, error) { return nil, nil } //nolint:nilnil // the misbehaving factory under test

	blobDataset, err := NewDataset("blob", factory)
	if err != nil {
		t.Fatal(err)
	}
	vol, err := NewVolume("vol", factory, 16)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("0123")))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := vol.Commit(ctx, []BlockRef{staged}, nil)
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
		{"NewVolume nil factory", func() error { _, err := NewVolume("v", nil, 1); return err }, "store factory"},
		{"NewVolume empty ID", func() error { _, err := NewVolume("", factory, 1); return err }, "volume ID"},
		{"NewVolume non-positive length", func() error { _, err := NewVolume("v", factory, 0); return err }, "total length"},
		{"NewVolume nil store", func() error { _, err := NewVolume("v", nilStore, 1); return err }, "nil store"},
		{"StageWriteAt negative offset", func() error { _, err := vol.StageWriteAt(ctx, -1, bytes.NewReader([]byte("x"))); return err }, "offset"},
		{"StageWriteAt empty block", func() error { _, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(nil)); return err }, "must not be empty"},
		{"StageWriteAt beyond volume", func() error { _, err := vol.StageWriteAt(ctx, 15, bytes.NewReader([]byte("xy"))); return err }, "address space"},
		{"Commit no blocks", func() error { _, err := vol.Commit(ctx, nil, nil); return err }, "at least one new block"},
		{"Commit already committed", func() error { _, err := vol.Commit(ctx, []BlockRef{staged}, nil); return err }, "already committed"},
		{"Commit negative offset", func() error {
			_, err := vol.Commit(ctx, []BlockRef{{Offset: -1, Length: 1, Path: "p"}}, nil)
			return err
		}, "offset"},
		{"Commit non-positive length", func() error {
			_, err := vol.Commit(ctx, []BlockRef{{Offset: 4, Length: 0, Path: "p"}}, nil)
			return err
		}, "length"},
		{"Commit beyond volume", func() error {
			_, err := vol.Commit(ctx, []BlockRef{{Offset: 4, Length: 100, Path: "p"}}, nil)
			return err
		}, "address space"},
		{"Commit foreign path", func() error {
			_, err := vol.Commit(ctx, []BlockRef{{Offset: 4, Length: 4, Path: "elsewhere"}}, nil)
			return err
		}, "layout path"},
		{"ReadAt negative offset", func() error { _, err := vol.ReadAt(ctx, snap.ID, -1, 1); return err }, "offset"},
		{"ReadAt non-positive length", func() error { _, err := vol.ReadAt(ctx, snap.ID, 0, 0); return err }, "length"},
		{"ReadAt beyond volume", func() error { _, err := vol.ReadAt(ctx, snap.ID, 0, 17); return err }, "address space"},
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
		{ErrOptionNotValidForVolume, "option not valid for volume"},
	}
	for _, c := range cases {
		if c.err.Error() != c.want {
			t.Errorf("%T: got %q, want %q", c.err, c.err.Error(), c.want)
		}
	}
}
