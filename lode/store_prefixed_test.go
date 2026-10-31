package lode

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"
)

// A prefixed store writes every object under its prefix in the inner store,
// lists only its own objects, and returns keys without the prefix.
func TestPrefixed_KeepsObjectsUnderThePrefix(t *testing.T) {
	ctx := t.Context()
	inner := NewMemory()
	store, err := Prefixed(Use(inner), "datasets/d/runs/r1")()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"volumes/a/latest", "volumes/a/data/0-3.bin"} {
		if err := store.Put(ctx, k, bytes.NewReader([]byte("abc"))); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	// A sibling that shares the prefix as a string, and an object outside it.
	for _, k := range []string{"datasets/d/runs/r10/volumes/b/latest", "datasets/d/other"} {
		if err := inner.Put(ctx, k, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatal(err)
		}
	}

	innerKeys, err := inner.List(ctx, "datasets/d/runs/r1/")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(innerKeys)
	if want := []string{"datasets/d/runs/r1/volumes/a/data/0-3.bin", "datasets/d/runs/r1/volumes/a/latest"}; !slices.Equal(innerKeys, want) {
		t.Fatalf("inner keys %q, want %q", innerKeys, want)
	}

	keys, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	if want := []string{"volumes/a/data/0-3.bin", "volumes/a/latest"}; !slices.Equal(keys, want) {
		t.Fatalf("listed %q, want %q", keys, want)
	}

	data, err := store.ReadRange(ctx, "volumes/a/data/0-3.bin", 1, 2)
	if err != nil || string(data) != "bc" {
		t.Fatalf("ReadRange = %q, %v", data, err)
	}
	if err := store.CompareAndSwap(ctx, "volumes/a/latest", "abc", "def"); err != nil {
		t.Fatalf("CompareAndSwap: %v", err)
	}
	rc, err := inner.Get(ctx, "datasets/d/runs/r1/volumes/a/latest")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "def" {
		t.Fatalf("the swap reached %q, want def", got)
	}
}

// A key that would leave the prefix is refused before the inner store sees
// it, by every operation.
func TestPrefixed_RefusesKeysThatLeaveThePrefix(t *testing.T) {
	ctx := t.Context()
	inner := NewMemory()
	if err := inner.Put(ctx, "secret", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	store, err := Prefixed(Use(inner), "a/b")()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"../../secret", "../b2/x", "", ".", `x\y`} {
		ops := map[string]error{}
		ops["Put"] = store.Put(ctx, k, bytes.NewReader([]byte("y")))
		_, ops["Get"] = store.Get(ctx, k)
		_, ops["Exists"] = store.Exists(ctx, k)
		ops["Delete"] = store.Delete(ctx, k)
		_, ops["ReadRange"] = store.ReadRange(ctx, k, 0, 1)
		_, ops["ReaderAt"] = store.ReaderAt(ctx, k)
		ops["CompareAndSwap"] = store.CompareAndSwap(ctx, k, "", "y")
		for op, err := range ops {
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("%s(%q) = %v, want ErrInvalidPath", op, k, err)
			}
		}
	}
	if _, err := store.List(ctx, "../"); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("List(../) = %v, want ErrInvalidPath", err)
	}
	if exists, _ := inner.Exists(ctx, "secret"); !exists {
		t.Fatal("an object outside the prefix was removed")
	}
}

// A prefix that names no location inside the root is refused when the
// factory is called, before the inner factory runs.
func TestPrefixed_RefusesAnInvalidPrefix(t *testing.T) {
	called := false
	inner := func() (RangeStore, error) { called = true; return NewMemory(), nil }
	for _, p := range []string{"", ".", "/", "..", "../x", `a\b`} {
		if _, err := Prefixed(inner, p)(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Prefixed(%q) = %v, want ErrInvalidArgument", p, err)
		}
	}
	if _, err := Prefixed[RangeStore](nil, "a")(); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Prefixed(nil) = %v, want ErrInvalidArgument", err)
	}
	if called {
		t.Fatal("the inner factory ran for an invalid prefix")
	}
}

// An error from the inner factory is returned as it is.
func TestPrefixed_ReturnsTheFactoryError(t *testing.T) {
	boom := errors.New("boom")
	if _, err := Prefixed(func() (RangeStore, error) { return nil, boom }, "a")(); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the factory's error", err)
	}
	nilStore := func() (RangeStore, error) { return nil, nil } //nolint:nilnil // the misbehaving factory under test
	if _, err := Prefixed(nilStore, "a")(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil store: got %v, want ErrInvalidArgument", err)
	}
	typedNil := func() (*memoryStore, error) { return nil, nil } //nolint:nilnil // the misbehaving factory under test
	if _, err := Prefixed(typedNil, "a")(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("typed nil store: got %v, want ErrInvalidArgument", err)
	}
}

// A factory of a concrete store type, as Use returns for a store the caller
// holds, is accepted without a conversion.
func TestPrefixed_AcceptsAConcreteStoreFactory(t *testing.T) {
	inner := &memoryStore{data: map[string][]byte{}}
	store, err := Prefixed(Use(inner), "p")()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), "k", bytes.NewReader([]byte("v"))); err != nil {
		t.Fatal(err)
	}
	if exists, _ := inner.Exists(t.Context(), "p/k"); !exists {
		t.Fatal("the object is not under the prefix in the concrete store")
	}
}
