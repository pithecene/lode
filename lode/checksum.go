package lode

import (
	"crypto/md5" //nolint:gosec // non-cryptographic integrity digest for persisted objects
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"hash/crc32"
)

// -----------------------------------------------------------------------------
// Bundled Checksums
// -----------------------------------------------------------------------------

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// SHA256 returns a SHA-256 checksum component (64 hex characters, wire name "sha256").
func SHA256() Checksum { return checksumAlgo{name: "sha256", newHash: sha256.New} }

// CRC32C returns a Castagnoli CRC-32C checksum component (8 hex characters, wire name "crc32c").
func CRC32C() Checksum {
	return checksumAlgo{name: "crc32c", newHash: func() hash.Hash { return crc32.New(crc32cTable) }}
}

// MD5 returns an MD5 checksum component (32 hex characters, wire name "md5").
func MD5() Checksum {
	return checksumAlgo{name: "md5", newHash: md5.New} //nolint:gosec // integrity checksum, not security hash
}

type checksumAlgo struct {
	name    string
	newHash func() hash.Hash
}

func (c checksumAlgo) Name() string { return c.name }

func (c checksumAlgo) NewHasher() HashWriter {
	return &hashWriter{h: c.newHash()}
}

// hashWriter wraps a hash.Hash to implement HashWriter.
type hashWriter struct {
	h hash.Hash
}

func (hw *hashWriter) Write(p []byte) (n int, err error) {
	return hw.h.Write(p)
}

func (hw *hashWriter) Sum() string {
	return hex.EncodeToString(hw.h.Sum(nil))
}
