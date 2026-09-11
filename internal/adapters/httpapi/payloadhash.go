package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// payloadHash is an interim stand-in for the canonical, field-normalized
// hash Fase 7 formalizes (documented field list, decimal normalization
// before hashing, HTTP/SQS equivalence). It hashes the deterministically
// ordered JSON encoding of the request's business fields — encoding/json
// always serializes a struct's fields in their declared order, so the same
// semantic request always hashes the same way — which is enough for
// Handle's same-key-same-hash idempotency check to work correctly now.
// idempotencyKey itself is never part of req, so it is excluded by
// construction.
func payloadHash(req wagerTransactionRequest) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
