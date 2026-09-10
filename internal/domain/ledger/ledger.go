// Package ledger implements WalletLedgerEntry: an immutable, append-only
// record of a single balance movement.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

// Direction is the movement direction of a ledger entry.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

var ErrInvalidEntry = errors.New("ledger: invalid entry")

// Entry is a single, immutable ledger record. There are no setters: once
// constructed, an Entry cannot be changed. balanceAfter is validated
// against balanceBefore and the movement's amount/direction at
// construction time, so an Entry can never be built in an inconsistent
// state.
type Entry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewParams carries the fields required to build a ledger entry.
type NewParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// New builds a ledger entry, validating that
// balanceAfter = balanceBefore ± amount according to direction. This is
// also the path used to reconstruct entries read back from storage: the
// same invariant is re-checked on every read, which doubles as a cheap
// integrity check.
func New(p NewParams) (*Entry, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return nil, fmt.Errorf("%w: id, walletID and transactionID are required", ErrInvalidEntry)
	}
	if err := p.Amount.RequirePositive(); err != nil {
		return nil, fmt.Errorf("%w: amount must be positive: %v", ErrInvalidEntry, err)
	}
	if p.BalanceBefore.Currency() != p.Amount.Currency() || p.BalanceAfter.Currency() != p.Amount.Currency() {
		return nil, fmt.Errorf("%w: currency mismatch between amount and balances", ErrInvalidEntry)
	}
	if p.CreatedAt.IsZero() {
		return nil, fmt.Errorf("%w: createdAt is required", ErrInvalidEntry)
	}

	var expected money.Money
	var err error
	switch p.Direction {
	case DirectionDebit:
		expected, err = p.BalanceBefore.Sub(p.Amount)
	case DirectionCredit:
		expected, err = p.BalanceBefore.Add(p.Amount)
	default:
		return nil, fmt.Errorf("%w: invalid direction %q", ErrInvalidEntry, p.Direction)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEntry, err)
	}
	if !expected.Equals(p.BalanceAfter) {
		return nil, fmt.Errorf("%w: balanceAfter does not match balanceBefore %s amount", ErrInvalidEntry, p.Direction)
	}

	return &Entry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		createdAt:     p.CreatedAt,
	}, nil
}

func (e *Entry) ID() uuid.UUID              { return e.id }
func (e *Entry) WalletID() uuid.UUID        { return e.walletID }
func (e *Entry) TransactionID() uuid.UUID   { return e.transactionID }
func (e *Entry) Direction() Direction       { return e.direction }
func (e *Entry) Amount() money.Money        { return e.amount }
func (e *Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *Entry) CreatedAt() time.Time       { return e.createdAt }
