package s3

import (
	"crypto/sha256"
	"sync"
)

// version identifies the content of an object, by its SHA-256 digest, and
// the ETag that S3 returned when this store replaced it. The digest keeps a
// record small whatever the size of the content.
type version struct {
	digest [sha256.Size]byte
	etag   string
}

// versions records, by full key, the version of each object that this store
// last replaced through CompareAndSwap. Only replaced objects are recorded,
// which in practice means the latest pointers, one per dataset or volume.
// The record holds at most maxVersions entries; when it is full, an
// arbitrary entry is evicted. A record lets the next CompareAndSwap on that
// key expect the recorded ETag instead of reading the object first.
// A record may be stale, because another writer may have replaced the object
// since; the conditional write then fails, and CompareAndSwap reads the
// object as it would without a record. A record therefore saves a request
// but never decides an outcome.
// maxVersions bounds the number of recorded versions. A store that serves
// more pointers than this reads some of them before replacing them, as it
// would without a record; no outcome depends on the bound.
const maxVersions = 1024

type versions struct {
	mu    sync.Mutex
	byKey map[string]version
}

func newVersions() *versions {
	return &versions{byKey: make(map[string]version)}
}

// etagFor returns the recorded ETag of key when its recorded content is
// expected.
func (v *versions) etagFor(key, expected string) (string, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	rec, ok := v.byKey[key]
	if !ok || rec.digest != sha256.Sum256([]byte(expected)) {
		return "", false
	}
	return rec.etag, true
}

// record notes that key now holds content under etag. Without an ETag (a
// store that does not return one), the record is dropped instead.
func (v *versions) record(key, content, etag string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if etag == "" {
		delete(v.byKey, key)
		return
	}
	if _, ok := v.byKey[key]; !ok && len(v.byKey) >= maxVersions {
		v.evictOne()
	}
	v.byKey[key] = version{digest: sha256.Sum256([]byte(content)), etag: etag}
}

// evictOne drops an arbitrary record; the caller holds the lock.
func (v *versions) evictOne() {
	for key := range v.byKey {
		delete(v.byKey, key)
		return
	}
}

// forget drops the record of key, whose current version is unknown.
func (v *versions) forget(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.byKey, key)
}
