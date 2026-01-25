package util

import (
	"crypto/sha256"
	"encoding/hex"
)

// CalculateHash calculates a SHA-256 hash of the given data
func CalculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
