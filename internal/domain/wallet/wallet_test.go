package wallet_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

func mustMoney(t *testing.T, amount string, currency money.Currency) money.Money {
	t.Helper()
	m, err := money.New(amount, currency)
	require.NoError(t, err)
	return m
}

func newTestWallet(t *testing.T, initial string) *wallet.Wallet {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, initial, money.BRL),
		Now:            time.Now(),
	})
	require.NoError(t, err)
	return w
}

func TestNew_Version1(t *testing.T) {
	w := newTestWallet(t, "100.00")
	require.Equal(t, int64(1), w.Version())
	require.Equal(t, "100.00", w.Balance().String())
}

func TestNew_RejectsNegativeInitialBalance(t *testing.T) {
	_, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "-1.00", money.BRL),
		Now:            time.Now(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, wallet.ErrInvalidWallet))
}

func TestNew_RejectsCurrencyMismatch(t *testing.T) {
	_, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "10.00", money.USD),
		Now:            time.Now(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, wallet.ErrCurrencyMismatch))
}

func TestDebit_ReducesBalanceAndBumpsVersion(t *testing.T) {
	w := newTestWallet(t, "100.00")
	before, after, err := w.Debit(mustMoney(t, "30.00", money.BRL), time.Now())
	require.NoError(t, err)
	require.Equal(t, "100.00", before.String())
	require.Equal(t, "70.00", after.String())
	require.Equal(t, "70.00", w.Balance().String())
	require.Equal(t, int64(2), w.Version())
}

func TestDebit_InsufficientBalanceDoesNotMutate(t *testing.T) {
	w := newTestWallet(t, "50.00")
	_, _, err := w.Debit(mustMoney(t, "80.00", money.BRL), time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wallet.ErrInsufficientBalance))

	// balance and version must be unchanged after a rejected debit
	require.Equal(t, "50.00", w.Balance().String())
	require.Equal(t, int64(1), w.Version())
}

func TestDebit_ExactBalanceAllowed(t *testing.T) {
	w := newTestWallet(t, "50.00")
	_, after, err := w.Debit(mustMoney(t, "50.00", money.BRL), time.Now())
	require.NoError(t, err)
	require.Equal(t, "0.00", after.String())
}

func TestCredit_IncreasesBalanceAndBumpsVersion(t *testing.T) {
	w := newTestWallet(t, "100.00")
	before, after, err := w.Credit(mustMoney(t, "25.00", money.BRL), time.Now())
	require.NoError(t, err)
	require.Equal(t, "100.00", before.String())
	require.Equal(t, "125.00", after.String())
	require.Equal(t, int64(2), w.Version())
}

func TestDebitCredit_RejectCurrencyMismatch(t *testing.T) {
	w := newTestWallet(t, "100.00")
	_, _, err := w.Debit(mustMoney(t, "10.00", money.USD), time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wallet.ErrCurrencyMismatch))

	_, _, err = w.Credit(mustMoney(t, "10.00", money.USD), time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wallet.ErrCurrencyMismatch))
}

func TestDebitCredit_RejectNonPositiveAmount(t *testing.T) {
	w := newTestWallet(t, "100.00")
	_, _, err := w.Debit(mustMoney(t, "0.00", money.BRL), time.Now())
	require.Error(t, err)

	_, _, err = w.Credit(mustMoney(t, "-1.00", money.BRL), time.Now())
	require.Error(t, err)
}

func TestVersionOnlyIncrementsOnBalanceChange(t *testing.T) {
	w := newTestWallet(t, "100.00")
	require.Equal(t, int64(1), w.Version())

	// a rejected debit must not touch the version
	_, _, err := w.Debit(mustMoney(t, "1000.00", money.BRL), time.Now())
	require.Error(t, err)
	require.Equal(t, int64(1), w.Version())

	// a successful movement bumps it exactly once
	_, _, err = w.Credit(mustMoney(t, "1.00", money.BRL), time.Now())
	require.NoError(t, err)
	require.Equal(t, int64(2), w.Version())
}

func TestRehydrate_DoesNotReapplyAnything(t *testing.T) {
	id := uuid.New()
	playerID := uuid.New()
	now := time.Now()

	w, err := wallet.Rehydrate(wallet.RehydrateParams{
		ID:        id,
		PlayerID:  playerID,
		Currency:  money.BRL,
		Balance:   mustMoney(t, "42.00", money.BRL),
		Version:   7,
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now,
	})
	require.NoError(t, err)
	require.Equal(t, id, w.ID())
	require.Equal(t, playerID, w.PlayerID())
	require.Equal(t, "42.00", w.Balance().String())
	require.Equal(t, int64(7), w.Version())
}

func TestRehydrate_RejectsNegativeBalance(t *testing.T) {
	_, err := wallet.Rehydrate(wallet.RehydrateParams{
		ID:        uuid.New(),
		PlayerID:  uuid.New(),
		Currency:  money.BRL,
		Balance:   mustMoney(t, "-1.00", money.BRL),
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	require.Error(t, err)
}
