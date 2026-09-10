// Package ports declares the interfaces application use cases depend on.
// No implementation lives here — adapters (Fase 4+) implement these against
// pgx, SQS, etc. Use cases never import an adapter package directly.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

// Sentinel errors every adapter implementation must return (wrapped, via
// fmt.Errorf("%w: ...", ports.ErrNotFound)) so use cases can branch on them
// with errors.Is regardless of which concrete repository is behind the
// interface.
var (
	ErrNotFound      = errors.New("ports: not found")
	ErrAlreadyExists = errors.New("ports: already exists")
)

// Clock abstracts wall-clock time so use cases are deterministic in tests.
type Clock interface {
	Now() time.Time
}

// IDGenerator abstracts identifier generation so use cases are deterministic
// in tests.
type IDGenerator interface {
	NewID() uuid.UUID
}

// UnitOfWork runs fn within a single SQL transaction. Every repository call
// made through the ctx fn receives must participate in that same
// transaction — concrete implementations thread the transaction handle via
// context, not as an explicit parameter, so ports stay free of any SQL
// library type.
type UnitOfWork interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// WalletRepository persists and retrieves wallets.
type WalletRepository interface {
	// GetForUpdate locks the wallet row for the remainder of the current
	// transaction (pessimistic concurrency control). Must only be called
	// from within UnitOfWork.WithinTx. Returns ErrNotFound if no wallet
	// exists with that id.
	GetForUpdate(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error)
	// GetByID reads a wallet without locking it, for read-only queries.
	GetByID(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error)
	GetByPlayerAndCurrency(ctx context.Context, playerID uuid.UUID, currency money.Currency) (*wallet.Wallet, error)
	// Insert persists a newly created wallet. Returns ErrAlreadyExists if
	// (playerID, currency) already has a wallet.
	Insert(ctx context.Context, w *wallet.Wallet) error
	// Update persists a mutated wallet (balance/version change made via
	// Wallet.Debit/Credit).
	Update(ctx context.Context, w *wallet.Wallet) error
}

// WagerTransactionRepository persists and retrieves wager transactions.
type WagerTransactionRepository interface {
	Insert(ctx context.Context, tx *wagertransaction.WagerTransaction) error
	Update(ctx context.Context, tx *wagertransaction.WagerTransaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*wagertransaction.WagerTransaction, error)
	// GetForUpdate locks the transaction row for the remainder of the
	// current transaction. Required before resuming a PENDING_REFERENCE
	// transaction (ports consumer: processwagertransaction.Service.Resume)
	// — without it, two instances resolving the same pending transaction
	// concurrently could both apply the reversal's movement.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wagertransaction.WagerTransaction, error)
	FindByIdempotencyKey(ctx context.Context, key string) (*wagertransaction.WagerTransaction, error)
	FindByProviderAndExternalID(ctx context.Context, providerID, externalTransactionID string) (*wagertransaction.WagerTransaction, error)
	// HasSuccessfulReversal reports whether a REFUND or ROLLBACK (per kind)
	// has already been PROCESSED against (providerID, referenceExternalID).
	// The schema enforces this as a hard uniqueness constraint (Fase 2);
	// checking it here lets the use case return a clean business rejection
	// instead of a raw constraint-violation error.
	HasSuccessfulReversal(ctx context.Context, providerID, referenceExternalID string, kind wagertransaction.Kind) (bool, error)
	// ListPendingReferenceForUpdate returns up to limit transactions in
	// PENDING_REFERENCE, locked for update, for the pending-reference
	// worker (Fase 10) to attempt resolution on.
	ListPendingReferenceForUpdate(ctx context.Context, limit int) ([]*wagertransaction.WagerTransaction, error)
}

// LedgerRepository appends and reads ledger entries.
type LedgerRepository interface {
	Append(ctx context.Context, entry *ledger.Entry) error
	ListByWallet(ctx context.Context, walletID uuid.UUID) ([]*ledger.Entry, error)
}

// InboxMessage identifies a single inbound message for durable
// deduplication.
type InboxMessage struct {
	ConsumerName string
	MessageID    string
	Hash         string
}

// InboxRepository provides at-least-once delivery deduplication for
// message-based entry points (SQS). Insert and completion share the same
// transaction as the domain change the message triggers.
type InboxRepository interface {
	// TryInsert inserts the inbox record if (ConsumerName, MessageID) is
	// not yet known. When it already exists, alreadyExists is true and no
	// error is returned — the caller decides how to react from there
	// (typically: skip reprocessing, return the previously recorded
	// result).
	TryInsert(ctx context.Context, msg InboxMessage) (alreadyExists bool, err error)
	MarkCompleted(ctx context.Context, consumerName, messageID string, completedAt time.Time) error
}

// OutboxRecord is a pending (or previously attempted) outbox row, as read
// back by ClaimBatch.
type OutboxRecord struct {
	EventID     uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
	Attempts    int
}

// OutboxRepository persists domain events for later publication by a
// separate worker (Fase 11), in the same transaction as the domain change
// that originated them. payload is the event's JSON-encoded snapshot.
type OutboxRepository interface {
	Enqueue(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte, occurredAt time.Time) error
	// ClaimBatch locks up to limit unpublished, due (next_attempt_at <= now)
	// rows for the caller identified by lockedBy, for at most lockDuration
	// before another publisher instance may claim them (recovering
	// abandoned work). Must be called from within UnitOfWork.WithinTx.
	ClaimBatch(ctx context.Context, limit int, lockedBy string, lockDuration time.Duration) ([]OutboxRecord, error)
	// MarkPublished records successful delivery, preserving the eventId —
	// republication after a crash between publish and this call must reuse
	// the same eventId, never generate a new one.
	MarkPublished(ctx context.Context, eventID uuid.UUID, publishedAt time.Time) error
}

// EventPublisher is the port the outbox worker (Fase 11) depends on to
// actually deliver an event to its destination (SQS). Use cases in this
// package never call it directly — they only ever write to the outbox.
type EventPublisher interface {
	Publish(ctx context.Context, eventType string, payload []byte) error
}
