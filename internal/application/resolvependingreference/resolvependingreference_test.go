package resolvependingreference_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func TestResolve_ReferenceArrivedLate_NowProcesses(t *testing.T) {
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	inbox := apptest.NewInboxRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	processor := processwagertransaction.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, inbox, outbox, clock, ids)
	svc := resolvependingreference.New(txs, processor)

	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, "100.00"),
		Now:            time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, wallets.Insert(context.Background(), w))

	playerID := w.PlayerID()

	// a REFUND arrives before its BET — persisted as PENDING_REFERENCE
	refundReq := processwagertransaction.Request{
		IdempotencyKey:                 "provider-a:refund-1",
		ProviderID:                     "provider-a",
		ExternalTransactionID:          "refund-1",
		PayloadHash:                    "hash-refund-1",
		WalletID:                       w.ID(),
		PlayerID:                       playerID,
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           wagertransaction.KindRefund,
		Amount:                         mustMoney(t, "25.00"),
		ReferenceExternalTransactionID: "bet-1",
	}
	pendingResult, err := processor.Handle(context.Background(), refundReq)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusPendingReference, pendingResult.Status)

	ready, err := svc.ListReady(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	require.Equal(t, pendingResult.TransactionID, ready[0].ID())

	// the referenced BET now arrives and is processed
	betReq := processwagertransaction.Request{
		IdempotencyKey:        "provider-a:bet-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-1",
		PayloadHash:           "hash-bet-1",
		WalletID:              w.ID(),
		PlayerID:              playerID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  wagertransaction.KindBet,
		Amount:                mustMoney(t, "25.00"),
	}
	betResult, err := processor.Handle(context.Background(), betReq)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, betResult.Status)
	require.Equal(t, "75.00", betResult.Balance.String())

	// now the worker resumes the pending REFUND
	resumed, err := svc.Resolve(context.Background(), pendingResult.TransactionID, "corr-2")
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, resumed.Status)
	require.Equal(t, "100.00", resumed.Balance.String())

	ready, err = svc.ListReady(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, ready, "no more PENDING_REFERENCE transactions left")
}
