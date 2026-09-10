package reconciliation_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func TestReconcile_ConsistentWallet(t *testing.T) {
	wallets := apptest.NewWalletRepository()
	ledgers := apptest.NewLedgerRepository()
	svc := reconciliation.New(apptest.NoopUnitOfWork{}, wallets, ledgers)

	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "1000.00"),
		Now:            time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, wallets.Insert(context.Background(), w))

	openingEntry, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      w.ID(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionCredit,
		Amount:        mustMoney(t, "1000.00"),
		BalanceBefore: mustMoney(t, "0.00"),
		BalanceAfter:  mustMoney(t, "1000.00"),
		CreatedAt:     time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, ledgers.Append(context.Background(), openingEntry))

	betBefore, betAfter, err := w.Debit(mustMoney(t, "25.00"), time.Now())
	require.NoError(t, err)
	require.NoError(t, wallets.Update(context.Background(), w))

	betEntry, err := ledger.New(ledger.NewParams{
		ID:            uuid.New(),
		WalletID:      w.ID(),
		TransactionID: uuid.New(),
		Direction:     ledger.DirectionDebit,
		Amount:        mustMoney(t, "25.00"),
		BalanceBefore: betBefore,
		BalanceAfter:  betAfter,
		CreatedAt:     time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, ledgers.Append(context.Background(), betEntry))

	result, err := svc.Reconcile(context.Background(), w.ID())
	require.NoError(t, err)
	require.True(t, result.Consistent)
	require.Equal(t, "0.00", result.Difference.String())
	require.Equal(t, "975.00", result.StoredBalance.String())
	require.Equal(t, "975.00", result.CalculatedBalance.String())
	require.Equal(t, 2, result.CheckedEntries)
}

func TestReconcile_DivergentWallet_ReportsDifference(t *testing.T) {
	wallets := apptest.NewWalletRepository()
	ledgers := apptest.NewLedgerRepository()
	svc := reconciliation.New(apptest.NoopUnitOfWork{}, wallets, ledgers)

	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "1000.00"),
		Now:            time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, wallets.Insert(context.Background(), w))

	// no ledger entry recorded for the opening at all — a divergence
	result, err := svc.Reconcile(context.Background(), w.ID())
	require.NoError(t, err)
	require.False(t, result.Consistent)
	require.Equal(t, "1000.00", result.Difference.String())
	require.Equal(t, 0, result.CheckedEntries)
}
