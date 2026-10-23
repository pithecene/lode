package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/history"
)

// Store fixtures shared by the tests of package lode.

func putObject(t *testing.T, store Store, p string) {
	t.Helper()
	if err := store.Put(t.Context(), p, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
}

// putManifestObject persists an object that begins as a manifest does, at
// a path of the caller's choosing.
func putManifestObject(t *testing.T, store Store, p string) {
	t.Helper()
	body := "{\n  \"schema_name\": \"lode-manifest\",\n  \"format_version\": \"1.1.0\"\n}"
	if err := store.Put(t.Context(), p, strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, store Store, p string) bool {
	t.Helper()
	ok, err := store.Exists(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// listInjector adds raw keys to listings, as a store that does not
// normalize its keys might return, hides keys from them, as a lagging
// listing would, fails one prefix's listing or one path's Get, and records
// deletes.
type listInjector struct {
	Store
	extra      []string
	hidden     []string
	deleted    []string
	failPrefix string // a listing of exactly this prefix fails
	failGet    string // a Get of exactly this path fails
	hideOnce   bool   // hidden applies to the first snapshot listing only, as one that catches up
	listings   int    // snapshot listings so far
}

func (l *listInjector) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	if l.failGet != "" && p == l.failGet {
		return nil, errGetFailed
	}
	return l.Store.Get(ctx, p)
}

var (
	errListFailed = errors.New("list failed")
	errGetFailed  = errors.New("get failed")
)

func (l *listInjector) List(ctx context.Context, prefix string) ([]string, error) {
	if l.failPrefix != "" && prefix == l.failPrefix {
		return nil, errListFailed
	}
	keys, err := l.Store.List(ctx, prefix)
	if !strings.Contains(prefix, "expired/") {
		l.listings++
	}
	if !l.hideOnce || l.listings == 1 {
		keys = slices.DeleteFunc(keys, func(k string) bool { return slices.Contains(l.hidden, k) })
	}
	for _, k := range l.extra {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, err
}

func (l *listInjector) Delete(ctx context.Context, p string) error {
	l.deleted = append(l.deleted, p)
	return l.Store.Delete(ctx, p)
}

func (l *listInjector) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	return l.Store.(ConditionalWriter).CompareAndSwap(ctx, p, expected, replacement)
}

func storeKeys(t *testing.T, store Store) []string {
	t.Helper()
	keys, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

func moveObject(t *testing.T, store Store, from, to string) {
	t.Helper()
	rc, err := store.Get(t.Context(), from)
	if err != nil {
		t.Fatal(err)
	}
	defer closer(rc)()
	if err := store.Put(t.Context(), to, rc); err != nil {
		t.Fatal(err)
	}
}

// idAt is the snapshot ID a write beginning at t would get, with a zero
// nonce.
func idAt(t time.Time) DatasetSnapshotID {
	return DatasetSnapshotID(history.FormatID(t, [history.NonceBytes]byte{}))
}

// emptyMetadata is the metadata of a write that supplied none, {}.
func emptyMetadata() Metadata {
	return Metadata{raw: emptyObject}
}

// validTestManifest is a minimal manifest of format 1.0.0 that passes
// validation.
func validTestManifest() *Manifest {
	return &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      emptyMetadata(),
		Files:         []FileRef{{Path: "data/f", SizeBytes: 1}},
		RowCount:      1,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
}

// writeManifest persists m at the default layout's canonical manifest path
// of its dataset and snapshot.
func writeManifest(ctx context.Context, t *testing.T, store Store, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := "datasets/" + string(m.DatasetID) + "/snapshots/" + string(m.SnapshotID) + "/manifest.json"
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}
