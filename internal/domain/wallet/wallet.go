// Package wallet implements Wallet, the financial aggregate root. Balance
// mutation is only ever performed through Debit/Credit, which keep the
// invariants (non-negative balance, version bump on every balance change)
// under the aggregate's own control.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidWallet       = errors.New("wallet: invalid state")
	ErrCurrencyMismatch    = errors.New("wallet: currency mismatch")
	ErrInsufficientBalance = errors.New("wallet: insufficient balance")
)

// Wallet is the aggregate root for a player's balance in a single currency.
// The pair (playerID, currency) identifies a unique wallet.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// NewParams carries the fields required to open a new wallet.
type NewParams struct {
	ID             uuid.UUID
	PlayerID       uuid.UUID
	Currency       money.Currency
	InitialBalance money.Money
	Now            time.Time
}

// New creates a wallet with version 1. The initial balance must not be
// negative; zero and positive balances are both allowed (a zero initial
// balance simply means no OPENING ledger entry is produced upstream, a
// decision made by the caller, not by this constructor).
func New(p NewParams) (*Wallet, error) {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id and playerID are required", ErrInvalidWallet)
	}
	if !p.Currency.Valid() {
		return nil, fmt.Errorf("%w: unsupported currency %q", ErrInvalidWallet, p.Currency)
	}
	if p.InitialBalance.Currency() != p.Currency {
		return nil, fmt.Errorf("%w: initial balance currency does not match wallet currency", ErrCurrencyMismatch)
	}
	if p.InitialBalance.IsNegative() {
		return nil, fmt.Errorf("%w: initial balance cannot be negative", ErrInvalidWallet)
	}
	if p.Now.IsZero() {
		return nil, fmt.Errorf("%w: now is required", ErrInvalidWallet)
	}

	return &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.Currency,
		balance:   p.InitialBalance,
		version:   1,
		createdAt: p.Now,
		updatedAt: p.Now,
	}, nil
}

// RehydrateParams carries the fields required to reconstruct a wallet
// exactly as persisted, without reapplying any movement.
type RehydrateParams struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Currency  money.Currency
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate reconstructs a wallet from persisted state. Unlike New, it does
// not emit any event or reapply any movement or transition — the caller
// supplies the state exactly as read from storage.
func Rehydrate(p RehydrateParams) (*Wallet, error) {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id and playerID are required", ErrInvalidWallet)
	}
	if !p.Currency.Valid() {
		return nil, fmt.Errorf("%w: unsupported currency %q", ErrInvalidWallet, p.Currency)
	}
	if p.Balance.Currency() != p.Currency {
		return nil, fmt.Errorf("%w: balance currency does not match wallet currency", ErrCurrencyMismatch)
	}
	if p.Balance.IsNegative() {
		return nil, fmt.Errorf("%w: persisted balance cannot be negative", ErrInvalidWallet)
	}
	if p.Version < 1 {
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidWallet)
	}

	return &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.Currency,
		balance:   p.Balance,
		version:   p.Version,
		createdAt: p.CreatedAt,
		updatedAt: p.UpdatedAt,
	}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

func (w *Wallet) validateMutation(amount money.Money) error {
	if amount.Currency() != w.currency {
		return fmt.Errorf("%w: movement in %s against wallet in %s", ErrCurrencyMismatch, amount.Currency(), w.currency)
	}
	if err := amount.RequirePositive(); err != nil {
		return err
	}
	return nil
}

// Debit subtracts amount from the balance, bumping the version and
// updatedAt timestamp. It returns the balance before and after the
// movement, for the caller to build the corresponding ledger entry in the
// same transaction. Debit never lets the balance go negative.
func (w *Wallet) Debit(amount money.Money, now time.Time) (balanceBefore, balanceAfter money.Money, err error) {
	if err := w.validateMutation(amount); err != nil {
		return money.Money{}, money.Money{}, err
	}
	newBalance, err := w.balance.Sub(amount)
	if err != nil {
		return money.Money{}, money.Money{}, err
	}
	if newBalance.IsNegative() {
		return money.Money{}, money.Money{}, ErrInsufficientBalance
	}

	before := w.balance
	w.balance = newBalance
	w.version++
	w.updatedAt = now
	return before, w.balance, nil
}

// Credit adds amount to the balance, bumping the version and updatedAt
// timestamp. It returns the balance before and after the movement, for the
// caller to build the corresponding ledger entry in the same transaction.
func (w *Wallet) Credit(amount money.Money, now time.Time) (balanceBefore, balanceAfter money.Money, err error) {
	if err := w.validateMutation(amount); err != nil {
		return money.Money{}, money.Money{}, err
	}
	newBalance, err := w.balance.Add(amount)
	if err != nil {
		return money.Money{}, money.Money{}, err
	}

	before := w.balance
	w.balance = newBalance
	w.version++
	w.updatedAt = now
	return before, w.balance, nil
}
