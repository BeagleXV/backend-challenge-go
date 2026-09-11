package ledger_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func TestNew_ValidDebit(t *testing.T) {
	e, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionDebit,
		Amount:        mustMoney(t, "30.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "70.00"),
		CreatedAt:     time.Now(),
	})
	require.NoError(t, err)
	require.Equal(t, ledger.DirectionDebit, e.Direction())
}

func TestNew_ValidCredit(t *testing.T) {
	e, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionCredit,
		Amount:        mustMoney(t, "30.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "130.00"),
		CreatedAt:     time.Now(),
	})
	require.NoError(t, err)
	require.Equal(t, ledger.DirectionCredit, e.Direction())
}

func TestNew_RejectsInconsistentBalance(t *testing.T) {
	_, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionDebit,
		Amount:        mustMoney(t, "30.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "80.00"), // should be 70.00
		CreatedAt:     time.Now(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ledger.ErrInvalidEntry))
}

func TestNew_RejectsNonPositiveAmount(t *testing.T) {
	_, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionCredit,
		Amount:        mustMoney(t, "0.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "100.00"),
		CreatedAt:     time.Now(),
	})
	require.Error(t, err)
}

func TestNew_RejectsInvalidDirection(t *testing.T) {
	_, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.Direction("SIDEWAYS"),
		Amount:        mustMoney(t, "10.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "110.00"),
		CreatedAt:     time.Now(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ledger.ErrInvalidEntry))
}

func TestNew_RejectsCurrencyMismatch(t *testing.T) {
	usd, err := money.New("30.00", money.USD)
	require.NoError(t, err)

	_, err = ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionDebit,
		Amount:        usd,
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "70.00"),
		CreatedAt:     time.Now(),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ledger.ErrInvalidEntry))
}

func TestNew_RequiresCreatedAt(t *testing.T) {
	_, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionDebit,
		Amount:        mustMoney(t, "30.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "70.00"),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ledger.ErrInvalidEntry))
}

func TestNew_RequiresIdentifiers(t *testing.T) {
	_, err := ledger.New(ledger.NewParams{
		WalletID:      uuid.New(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionCredit,
		Amount:        mustMoney(t, "10.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "110.00"),
		CreatedAt:     time.Now(),
	})
	require.Error(t, err)
}
