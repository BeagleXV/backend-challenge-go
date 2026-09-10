package openwallet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

type harness struct {
	svc     *openwallet.Service
	wallets *apptest.WalletRepository
	txs     *apptest.WagerTransactionRepository
	ledgers *apptest.LedgerRepository
	outbox  *apptest.OutboxRepository
}

func newHarness() *harness {
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	svc := openwallet.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, outbox, clock, ids)
	return &harness{svc: svc, wallets: wallets, txs: txs, ledgers: ledgers, outbox: outbox}
}

func TestHandle_ZeroInitialBalance_CreatesOnlyWallet(t *testing.T) {
	h := newHarness()
	req := openwallet.Request{
		WalletID:       uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "0.00"),
	}

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, req.WalletID, result.WalletID)
	require.Equal(t, "0.00", result.Balance.String())
	require.Equal(t, int64(1), result.Version)

	entries, _ := h.ledgers.ListByWallet(context.Background(), req.WalletID)
	require.Empty(t, entries, "zero initial balance must not produce a ledger entry")
	require.Empty(t, h.outbox.Events, "zero initial balance must not produce outbox events")
}

func TestHandle_PositiveInitialBalance_CreatesOpeningLedgerAndEvents(t *testing.T) {
	h := newHarness()
	req := openwallet.Request{
		WalletID:       uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "1000.00"),
		CorrelationID:  "corr-1",
	}

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "1000.00", result.Balance.String())
	require.Equal(t, int64(1), result.Version)

	entries, _ := h.ledgers.ListByWallet(context.Background(), req.WalletID)
	require.Len(t, entries, 1)
	require.Equal(t, "1000.00", entries[0].Amount().String())
	require.Equal(t, "0.00", entries[0].BalanceBefore().String())
	require.Equal(t, "1000.00", entries[0].BalanceAfter().String())

	require.Len(t, h.outbox.Events, 2, "expected WagerTransactionProcessed and WalletBalanceChanged")
	eventTypes := map[string]bool{}
	for _, e := range h.outbox.Events {
		eventTypes[e.EventType] = true
	}
	require.True(t, eventTypes["WagerTransactionProcessed"])
	require.True(t, eventTypes["WalletBalanceChanged"])
}

func TestHandle_DuplicateWalletForPlayerAndCurrency_Conflicts(t *testing.T) {
	h := newHarness()
	playerID := uuid.New()
	req1 := openwallet.Request{
		WalletID:       uuid.New(),
		PlayerID:       playerID,
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "0.00"),
	}
	_, err := h.svc.Handle(context.Background(), req1)
	require.NoError(t, err)

	req2 := openwallet.Request{
		WalletID:       uuid.New(),
		PlayerID:       playerID,
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "0.00"),
	}
	_, err = h.svc.Handle(context.Background(), req2)
	require.Error(t, err)
	require.True(t, errors.Is(err, openwallet.ErrWalletAlreadyExists))
}

func TestHandle_CurrencyMismatch_Rejected(t *testing.T) {
	h := newHarness()
	req := openwallet.Request{
		WalletID:       uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "10.00"), // BRL amount but...
	}
	req.Currency = money.USD // ...wallet declared as USD
	_, err := h.svc.Handle(context.Background(), req)
	require.Error(t, err)
	require.True(t, errors.Is(err, openwallet.ErrInvalidRequest))
}
