package event_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/event"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func TestNewWagerTransactionProcessed(t *testing.T) {
	txID := uuid.New()
	walletID := uuid.New()

	evt, err := event.NewWagerTransactionProcessed(uuid.New(), txID, "corr-1", "", time.Now(), event.WagerTransactionProcessedData{
		TransactionID: txID,
		WalletID:      walletID,
		ProviderID:    "provider-a",
		Kind:          "BET",
		Amount:        mustMoney(t, "25.00"),
	})
	require.NoError(t, err)
	require.Equal(t, event.TypeWagerTransactionProcessed, evt.EventType)
	require.Equal(t, 1, evt.Version)
}

func TestNewWagerTransactionProcessed_RequiresIdentifiers(t *testing.T) {
	_, err := event.NewWagerTransactionProcessed(uuid.New(), uuid.New(), "corr-1", "", time.Now(), event.WagerTransactionProcessedData{
		Amount: mustMoney(t, "25.00"),
	})
	require.Error(t, err)
}

func TestNewWagerTransactionRejected_RequiresFailureCode(t *testing.T) {
	txID := uuid.New()
	_, err := event.NewWagerTransactionRejected(uuid.New(), txID, "corr-1", "", time.Now(), event.WagerTransactionRejectedData{
		TransactionID: txID,
	})
	require.Error(t, err)
}

func TestNewWalletBalanceChanged_RequiresWalletVersion(t *testing.T) {
	walletID := uuid.New()
	txID := uuid.New()
	_, err := event.NewWalletBalanceChanged(uuid.New(), walletID, "corr-1", "", time.Now(), event.WalletBalanceChangedData{
		WalletID:      walletID,
		TransactionID: txID,
		Direction:     ledger.DirectionDebit,
		Amount:        mustMoney(t, "25.00"),
		BalanceBefore: mustMoney(t, "100.00"),
		BalanceAfter:  mustMoney(t, "75.00"),
		WalletVersion: 0,
	})
	require.Error(t, err)
}

func TestNewWagerTransactionPendingReference_RequiresReference(t *testing.T) {
	txID := uuid.New()
	_, err := event.NewWagerTransactionPendingReference(uuid.New(), txID, "corr-1", "", time.Now(), event.WagerTransactionPendingReferenceData{
		TransactionID: txID,
	})
	require.Error(t, err)
}

func TestEnvelope_RequiresCorrelationID(t *testing.T) {
	txID := uuid.New()
	_, err := event.NewWagerTransactionProcessed(uuid.New(), txID, "", "", time.Now(), event.WagerTransactionProcessedData{
		TransactionID: txID,
		WalletID:      uuid.New(),
		Amount:        mustMoney(t, "25.00"),
	})
	require.Error(t, err)
}
