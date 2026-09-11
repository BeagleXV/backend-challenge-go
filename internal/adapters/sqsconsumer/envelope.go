package sqsconsumer

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

// envelope is the wire shape every message on wager-transactions.fifo
// carries, per the contract (README section 10). messageId is the
// envelope's own durable identity — used for inbox dedup — distinct from
// data.idempotencyKey, which is the business idempotency key Handle uses
// exactly as it would for an HTTP submission.
type envelope struct {
	MessageID  string       `json:"messageId"`
	Type       string       `json:"type"`
	OccurredAt time.Time    `json:"occurredAt"`
	Data       envelopeData `json:"data"`
}

type envelopeData struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

const consumerName = "wager-consumer"

// toRequest builds the same processwagertransaction.Request shape the
// HTTP handler builds from its own wire format — this is what makes HTTP
// and SQS share every idempotency/business guarantee Handle provides, per
// Fase 7's CanonicalHash design: neither transport computes a hash itself,
// both just build a Request and call Handle. hash is the raw envelope
// body's content hash, used only for the inbox redelivery check — a
// different concern from CanonicalHash, which covers business fields.
func (e envelope) toRequest(hash string) (processwagertransaction.Request, error) {
	if e.MessageID == "" {
		return processwagertransaction.Request{}, fmt.Errorf("sqsconsumer: envelope missing messageId")
	}

	return processwagertransaction.Request{
		IdempotencyKey:                 e.Data.IdempotencyKey,
		ProviderID:                     e.Data.ProviderID,
		ExternalTransactionID:          e.Data.ExternalTransactionID,
		WalletID:                       e.Data.WalletID,
		PlayerID:                       e.Data.PlayerID,
		RoundID:                        e.Data.RoundID,
		GameID:                         e.Data.GameID,
		Kind:                           wagertransaction.Kind(e.Data.Kind),
		Amount:                         e.Data.Money,
		ReferenceExternalTransactionID: e.Data.ReferenceExternalTransactionID,
		CorrelationID:                  e.MessageID,
		Inbox: &processwagertransaction.InboxInfo{
			ConsumerName: consumerName,
			MessageID:    e.MessageID,
			Hash:         hash,
		},
	}, nil
}
