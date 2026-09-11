package processwagertransaction

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// canonicalPayload is the exact, fixed set of business fields that
// identify a wager operation, in a fixed field order. encoding/json always
// serializes a struct's fields in their declared order (never alphabetical
// re-sorting, never map-iteration order), so marshaling this struct is
// itself the "JSON canônico com ordenação de chaves" the contract asks
// for — no separate key-sorting step is needed.
//
// Deliberately excluded: IdempotencyKey (that's the lookup key, not part
// of what it identifies — hashing it would make every key its own trivial
// hash), CorrelationID and Inbox (transport/observability metadata, never
// business data), and PayloadHash itself.
//
// Amount is money.Money.String() — the fixed two-decimal decimal form
// ("25.00") money.New already normalizes every accepted amount to before
// Request ever reaches this function (so "25" and "25.00" hash
// identically), never a float and never the raw external string.
type canonicalPayload struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

// CanonicalHash computes the deterministic SHA-256 hash of req's business
// fields. HTTP and SQS both build the same Request shape from their own
// wire formats and call Handle, which calls this — there is exactly one
// place that decides what "the same operation" means, so the two
// transports can never diverge in what they consider a replay versus a
// conflict.
func CanonicalHash(req Request) string {
	payload := canonicalPayload{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		PlayerID:                       req.PlayerID.String(),
		WalletID:                       req.WalletID.String(),
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           string(req.Kind),
		Amount:                         req.Amount.String(),
		Currency:                       string(req.Amount.Currency()),
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	}
	// A struct of plain strings can never fail to marshal.
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
