// Package event defines the concrete domain event types produced by wager
// transaction processing: WagerTransactionProcessed, WagerTransactionRejected,
// WalletBalanceChanged and WagerTransactionPendingReference. These are the
// domain-level payloads; how they reach the transactional outbox is an
// application/adapter concern.
package event

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

// Type identifies the concrete event type.
type Type string

const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

// schemaVersion is the envelope version set by every event constructor in
// this package. Bump it if the envelope or a payload shape changes.
const schemaVersion = 1

var ErrInvalidEvent = errors.New("event: invalid event")

// Envelope carries the fields common to every event, per the required
// shape: eventId, eventType, aggregateId, correlationId, an optional
// causationId, occurredAt, version and a typed data payload (embedded by
// each concrete event type below).
type Envelope struct {
	ID            uuid.UUID
	EventType     Type
	AggregateID   uuid.UUID
	CorrelationID string
	CausationID   string // optional
	OccurredAt    time.Time
	Version       int
}

func newEnvelope(id uuid.UUID, eventType Type, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time) (Envelope, error) {
	if id == uuid.Nil {
		return Envelope{}, fmt.Errorf("%w: id is required", ErrInvalidEvent)
	}
	if aggregateID == uuid.Nil {
		return Envelope{}, fmt.Errorf("%w: aggregateID is required", ErrInvalidEvent)
	}
	if correlationID == "" {
		return Envelope{}, fmt.Errorf("%w: correlationID is required", ErrInvalidEvent)
	}
	if occurredAt.IsZero() {
		return Envelope{}, fmt.Errorf("%w: occurredAt is required", ErrInvalidEvent)
	}
	return Envelope{
		ID:            id,
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAt,
		Version:       schemaVersion,
	}, nil
}

// WagerTransactionProcessedData is the payload for a successfully
// concluded operation, including LOSS.
type WagerTransactionProcessedData struct {
	TransactionID         uuid.UUID
	WalletID              uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	Kind                  string
	Amount                money.Money
}

// WagerTransactionProcessed is emitted on successful conclusion of any
// operation, including LOSS (which carries no balance movement).
type WagerTransactionProcessed struct {
	Envelope
	Data WagerTransactionProcessedData
}

func NewWagerTransactionProcessed(id, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time, data WagerTransactionProcessedData) (WagerTransactionProcessed, error) {
	env, err := newEnvelope(id, TypeWagerTransactionProcessed, aggregateID, correlationID, causationID, occurredAt)
	if err != nil {
		return WagerTransactionProcessed{}, err
	}
	if data.TransactionID == uuid.Nil || data.WalletID == uuid.Nil {
		return WagerTransactionProcessed{}, fmt.Errorf("%w: transactionID and walletID are required", ErrInvalidEvent)
	}
	return WagerTransactionProcessed{Envelope: env, Data: data}, nil
}

// WagerTransactionRejectedData is the payload for a definitive
// business-rule refusal.
type WagerTransactionRejectedData struct {
	TransactionID         uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	Kind                  string
	FailureCode           string
}

// WagerTransactionRejected is emitted on a definitive business-rule
// refusal.
type WagerTransactionRejected struct {
	Envelope
	Data WagerTransactionRejectedData
}

func NewWagerTransactionRejected(id, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time, data WagerTransactionRejectedData) (WagerTransactionRejected, error) {
	env, err := newEnvelope(id, TypeWagerTransactionRejected, aggregateID, correlationID, causationID, occurredAt)
	if err != nil {
		return WagerTransactionRejected{}, err
	}
	if data.TransactionID == uuid.Nil {
		return WagerTransactionRejected{}, fmt.Errorf("%w: transactionID is required", ErrInvalidEvent)
	}
	if data.FailureCode == "" {
		return WagerTransactionRejected{}, fmt.Errorf("%w: failureCode is required", ErrInvalidEvent)
	}
	return WagerTransactionRejected{Envelope: env, Data: data}, nil
}

// WalletBalanceChangedData is the payload for an effective balance change.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     ledger.Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
}

// WalletBalanceChanged is emitted whenever a wallet's balance effectively
// changes (never for LOSS, which carries no movement).
type WalletBalanceChanged struct {
	Envelope
	Data WalletBalanceChangedData
}

func NewWalletBalanceChanged(id, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time, data WalletBalanceChangedData) (WalletBalanceChanged, error) {
	env, err := newEnvelope(id, TypeWalletBalanceChanged, aggregateID, correlationID, causationID, occurredAt)
	if err != nil {
		return WalletBalanceChanged{}, err
	}
	if data.WalletID == uuid.Nil || data.TransactionID == uuid.Nil {
		return WalletBalanceChanged{}, fmt.Errorf("%w: walletID and transactionID are required", ErrInvalidEvent)
	}
	if data.WalletVersion < 1 {
		return WalletBalanceChanged{}, fmt.Errorf("%w: walletVersion must be >= 1", ErrInvalidEvent)
	}
	return WalletBalanceChanged{Envelope: env, Data: data}, nil
}

// WagerTransactionPendingReferenceData is the payload for a registered wait
// on a not-yet-available reference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	Kind                           string
	ReferenceExternalTransactionID string
}

// WagerTransactionPendingReference is emitted when an operation is
// persisted as PENDING_REFERENCE, waiting for its reference to arrive.
type WagerTransactionPendingReference struct {
	Envelope
	Data WagerTransactionPendingReferenceData
}

func NewWagerTransactionPendingReference(id, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time, data WagerTransactionPendingReferenceData) (WagerTransactionPendingReference, error) {
	env, err := newEnvelope(id, TypeWagerTransactionPendingReference, aggregateID, correlationID, causationID, occurredAt)
	if err != nil {
		return WagerTransactionPendingReference{}, err
	}
	if data.TransactionID == uuid.Nil {
		return WagerTransactionPendingReference{}, fmt.Errorf("%w: transactionID is required", ErrInvalidEvent)
	}
	if data.ReferenceExternalTransactionID == "" {
		return WagerTransactionPendingReference{}, fmt.Errorf("%w: referenceExternalTransactionID is required", ErrInvalidEvent)
	}
	return WagerTransactionPendingReference{Envelope: env, Data: data}, nil
}
