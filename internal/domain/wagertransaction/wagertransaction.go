// Package wagertransaction implements WagerTransaction, including its
// state machine and the split between internal (OPENING) and external
// (BET/WIN/LOSS/REFUND/ROLLBACK) origins.
package wagertransaction

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

// Kind identifies the type of wager operation.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// Origin distinguishes the internal wallet-opening operation from
// provider-originated external operations. The schema (and this type) must
// prevent the two from being confused.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// Status is a state in the WagerTransaction state machine.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// IsTerminal reports whether s is one of the three terminal states. A
// transaction in a terminal state must never transition again.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusProcessed, StatusRejected, StatusFailed:
		return true
	default:
		return false
	}
}

// validTransitions enumerates every allowed edge in the state machine.
// PENDING covers operations accepted but not yet concluded; from there the
// application may resolve synchronously (-> PROCESSED/REJECTED/FAILED) or
// discover it depends on a not-yet-available reference
// (-> PENDING_REFERENCE), which a dedicated worker retries with backoff
// until it resolves or expires (-> PROCESSED/REJECTED/FAILED). FAILED marks
// a permanent infrastructure failure recorded for audit, distinct from
// REJECTED (a business-rule refusal).
var validTransitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusPendingReference: true,
		StatusProcessed:        true,
		StatusRejected:         true,
		StatusFailed:           true,
	},
	StatusPendingReference: {
		StatusProcessed: true,
		StatusRejected:  true,
		StatusFailed:    true,
	},
}

var (
	ErrInvalidWagerTransaction = errors.New("wagertransaction: invalid state")
	ErrInvalidKind             = errors.New("wagertransaction: invalid kind")
	ErrInvalidTransition       = errors.New("wagertransaction: invalid state transition")
	ErrReferenceRequired       = errors.New("wagertransaction: reference external transaction id is required")
	ErrReferenceNotApplicable  = errors.New("wagertransaction: reference not applicable to this kind")
	ErrFailureCodeRequired     = errors.New("wagertransaction: failure code is required")
)

// WagerTransaction models a single wager operation, either the internal
// wallet OPENING or one of the five external kinds.
type WagerTransaction struct {
	id     uuid.UUID
	origin Origin
	kind   Kind
	status Status

	// External-only metadata. Left at zero value for OPENING.
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	payloadHash                    string
	roundID                        string
	gameID                         string
	referenceExternalTransactionID string
	resolvedReferenceID            uuid.UUID
	failureCode                    string

	walletID uuid.UUID
	playerID uuid.UUID
	amount   money.Money

	// resultBalance is the wallet balance observed at the moment this
	// transaction reached PROCESSED or REJECTED — the "resultado financeiro
	// retornado ao provedor" the README requires persisting. nil until set.
	// An idempotent replay must return this exact snapshot, never the
	// wallet's current balance, which may have moved since.
	resultBalance *money.Money

	createdAt time.Time
	updatedAt time.Time
}

func (t *WagerTransaction) ID() uuid.UUID                 { return t.id }
func (t *WagerTransaction) Origin() Origin                { return t.origin }
func (t *WagerTransaction) Kind() Kind                    { return t.kind }
func (t *WagerTransaction) Status() Status                { return t.status }
func (t *WagerTransaction) ProviderID() string            { return t.providerID }
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t *WagerTransaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string           { return t.payloadHash }
func (t *WagerTransaction) RoundID() string               { return t.roundID }
func (t *WagerTransaction) GameID() string                { return t.gameID }
func (t *WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}
func (t *WagerTransaction) ResolvedReferenceID() uuid.UUID { return t.resolvedReferenceID }
func (t *WagerTransaction) FailureCode() string            { return t.failureCode }
func (t *WagerTransaction) WalletID() uuid.UUID            { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID            { return t.playerID }
func (t *WagerTransaction) Amount() money.Money            { return t.amount }
func (t *WagerTransaction) CreatedAt() time.Time           { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time           { return t.updatedAt }

// ResultBalance returns the wallet balance snapshot recorded when this
// transaction was finalized, and whether one was ever recorded (it never is
// for a transaction still PENDING/PENDING_REFERENCE, or FAILED without a
// financial outcome).
func (t *WagerTransaction) ResultBalance() (money.Money, bool) {
	if t.resultBalance == nil {
		return money.Money{}, false
	}
	return *t.resultBalance, true
}

// SetResultBalance records the wallet balance to report back for this
// transaction from now on, including on idempotent replay. Callers set this
// once, right before transitioning to PROCESSED or REJECTED.
func (t *WagerTransaction) SetResultBalance(balance money.Money) {
	t.resultBalance = &balance
}

// validateAmountForKind enforces the zero-value policy per kind: LOSS
// carries no movement and must be exactly "0.00"; BET, WIN, REFUND and
// ROLLBACK all require a strictly positive amount.
func validateAmountForKind(kind Kind, amount money.Money) error {
	switch kind {
	case KindLoss:
		if err := amount.RequireZero(); err != nil {
			return fmt.Errorf("%w: LOSS requires a zero amount: %v", ErrInvalidWagerTransaction, err)
		}
	case KindBet, KindWin, KindRefund, KindRollback:
		if err := amount.RequirePositive(); err != nil {
			return fmt.Errorf("%w: %s requires a positive amount: %v", ErrInvalidWagerTransaction, kind, err)
		}
	default:
		return fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
	return nil
}

func isReversal(kind Kind) bool {
	return kind == KindRefund || kind == KindRollback
}

// NewExternalParams carries the fields required to accept a
// provider-originated external operation.
type NewExternalParams struct {
	ID                             uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
	Now                            time.Time
}

// NewExternal creates a new external WagerTransaction in status PENDING.
// OPENING is rejected here — it is reserved for NewInternalOpening and must
// never be accepted from HTTP or SQS.
func NewExternal(p NewExternalParams) (*WagerTransaction, error) {
	if p.Kind == KindOpening {
		return nil, fmt.Errorf("%w: OPENING is reserved for internal origin", ErrInvalidKind)
	}
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id, walletID and playerID are required", ErrInvalidWagerTransaction)
	}
	if p.ProviderID == "" || p.ExternalTransactionID == "" || p.IdempotencyKey == "" || p.PayloadHash == "" {
		return nil, fmt.Errorf("%w: providerID, externalTransactionID, idempotencyKey and payloadHash are required for external operations", ErrInvalidWagerTransaction)
	}
	if p.RoundID == "" || p.GameID == "" {
		return nil, fmt.Errorf("%w: roundID and gameID are required for external operations", ErrInvalidWagerTransaction)
	}
	if err := validateAmountForKind(p.Kind, p.Amount); err != nil {
		return nil, err
	}
	if isReversal(p.Kind) && p.ReferenceExternalTransactionID == "" {
		return nil, ErrReferenceRequired
	}
	if !isReversal(p.Kind) && p.ReferenceExternalTransactionID != "" {
		return nil, ErrReferenceNotApplicable
	}
	if p.Now.IsZero() {
		return nil, fmt.Errorf("%w: now is required", ErrInvalidWagerTransaction)
	}

	return &WagerTransaction{
		id:                             p.ID,
		origin:                         OriginExternal,
		kind:                           p.Kind,
		status:                         StatusPending,
		providerID:                     p.ProviderID,
		externalTransactionID:          p.ExternalTransactionID,
		idempotencyKey:                 p.IdempotencyKey,
		payloadHash:                    p.PayloadHash,
		roundID:                        p.RoundID,
		gameID:                         p.GameID,
		referenceExternalTransactionID: p.ReferenceExternalTransactionID,
		walletID:                       p.WalletID,
		playerID:                       p.PlayerID,
		amount:                         p.Amount,
		createdAt:                      p.Now,
		updatedAt:                      p.Now,
	}, nil
}

// NewInternalOpeningParams carries the fields required for the internal
// wallet-opening operation.
type NewInternalOpeningParams struct {
	ID       uuid.UUID
	WalletID uuid.UUID
	PlayerID uuid.UUID
	Amount   money.Money
	Now      time.Time
}

// NewInternalOpening creates the internal OPENING operation. Provider,
// external id, idempotency key/hash, round, game and reference do not apply
// to this origin and are left unset. The amount may be zero (the caller
// decides whether a zero initial balance should produce an OPENING at all
// — this constructor does not enforce that policy).
func NewInternalOpening(p NewInternalOpeningParams) (*WagerTransaction, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id, walletID and playerID are required", ErrInvalidWagerTransaction)
	}
	if p.Amount.IsNegative() {
		return nil, fmt.Errorf("%w: opening amount cannot be negative", ErrInvalidWagerTransaction)
	}
	if p.Now.IsZero() {
		return nil, fmt.Errorf("%w: now is required", ErrInvalidWagerTransaction)
	}

	return &WagerTransaction{
		id:        p.ID,
		origin:    OriginInternal,
		kind:      KindOpening,
		status:    StatusPending,
		walletID:  p.WalletID,
		playerID:  p.PlayerID,
		amount:    p.Amount,
		createdAt: p.Now,
		updatedAt: p.Now,
	}, nil
}

// RehydrateParams carries the full persisted state of a WagerTransaction.
type RehydrateParams struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ResolvedReferenceID            uuid.UUID
	FailureCode                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Amount                         money.Money
	ResultBalance                  *money.Money
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// Rehydrate reconstructs a WagerTransaction exactly as persisted. It does
// not reapply any transition or movement and does not emit any event.
func Rehydrate(p RehydrateParams) (*WagerTransaction, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id, walletID and playerID are required", ErrInvalidWagerTransaction)
	}
	return &WagerTransaction{
		id:                             p.ID,
		origin:                         p.Origin,
		kind:                           p.Kind,
		status:                         p.Status,
		providerID:                     p.ProviderID,
		externalTransactionID:          p.ExternalTransactionID,
		idempotencyKey:                 p.IdempotencyKey,
		payloadHash:                    p.PayloadHash,
		roundID:                        p.RoundID,
		gameID:                         p.GameID,
		referenceExternalTransactionID: p.ReferenceExternalTransactionID,
		resolvedReferenceID:            p.ResolvedReferenceID,
		failureCode:                    p.FailureCode,
		walletID:                       p.WalletID,
		playerID:                       p.PlayerID,
		amount:                         p.Amount,
		resultBalance:                  p.ResultBalance,
		createdAt:                      p.CreatedAt,
		updatedAt:                      p.UpdatedAt,
	}, nil
}

func (t *WagerTransaction) transitionTo(next Status, now time.Time) error {
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s is a terminal state", ErrInvalidTransition, t.status)
	}
	allowed, ok := validTransitions[t.status]
	if !ok || !allowed[next] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, next)
	}
	t.status = next
	t.updatedAt = now
	return nil
}

// MarkPendingReference transitions PENDING -> PENDING_REFERENCE, used when
// processing depends on a reference that has not arrived yet.
func (t *WagerTransaction) MarkPendingReference(now time.Time) error {
	return t.transitionTo(StatusPendingReference, now)
}

// MarkProcessed transitions to the PROCESSED terminal state.
func (t *WagerTransaction) MarkProcessed(now time.Time) error {
	return t.transitionTo(StatusProcessed, now)
}

// MarkRejected transitions to the REJECTED terminal state with a stable,
// documented failure code (a business-rule refusal, not an infrastructure
// failure).
func (t *WagerTransaction) MarkRejected(failureCode string, now time.Time) error {
	if failureCode == "" {
		return ErrFailureCodeRequired
	}
	if err := t.transitionTo(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = failureCode
	return nil
}

// MarkFailed transitions to the FAILED terminal state with a stable
// failure code, recording a permanent infrastructure failure for audit.
func (t *WagerTransaction) MarkFailed(failureCode string, now time.Time) error {
	if failureCode == "" {
		return ErrFailureCodeRequired
	}
	if err := t.transitionTo(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = failureCode
	return nil
}

// ResolveReference records the internal id of the transaction this REFUND
// or ROLLBACK resolved against. It does not change status by itself — the
// caller still calls MarkProcessed/MarkRejected/MarkFailed as appropriate.
func (t *WagerTransaction) ResolveReference(resolvedID uuid.UUID, now time.Time) error {
	if !isReversal(t.kind) {
		return fmt.Errorf("%w: %s", ErrReferenceNotApplicable, t.kind)
	}
	if resolvedID == uuid.Nil {
		return fmt.Errorf("%w: resolved reference id is required", ErrInvalidWagerTransaction)
	}
	t.resolvedReferenceID = resolvedID
	t.updatedAt = now
	return nil
}
