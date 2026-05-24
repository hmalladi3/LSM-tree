// Package crc wraps hash/crc32 with the Castagnoli polynomial.
//
// CRC-32C (Castagnoli) is the polynomial used by every on-disk frame in slate:
// the manifest, the WAL, the value log, and every SSTable block. Castagnoli is
// hardware-accelerated on every target CPU (SSE 4.2 on x86, NEON on ARM) and
// detects all single-bit errors plus virtually all multi-bit corruption that
// real storage media produce.
package crc

import "hash/crc32"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Compute returns the CRC-32C of b.
func Compute(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// Update returns the CRC-32C of b appended to the running value crc.
//
// Useful for streaming computation (e.g., backup's rolling CRC).
func Update(crc uint32, b []byte) uint32 {
	return crc32.Update(crc, castagnoli, b)
}
