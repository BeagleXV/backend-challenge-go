package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ledgerCursor is the opaque pagination cursor for GET
// /wallets/:walletId/ledger. It encodes the keyset position
// (createdAt, id) the client last saw — never a raw sequential id, so it
// carries no exploitable internal structure across providers.
type ledgerCursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        uuid.UUID `json:"id"`
}

func encodeLedgerCursor(c ledgerCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodeLedgerCursor(raw string) (ledgerCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return ledgerCursor{}, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var c ledgerCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return ledgerCursor{}, fmt.Errorf("invalid cursor content: %w", err)
	}
	return c, nil
}
