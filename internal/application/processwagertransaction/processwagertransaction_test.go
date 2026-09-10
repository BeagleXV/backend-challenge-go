package processwagertransaction_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
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

type harness struct {
	svc     *processwagertransaction.Service
	wallets *apptest.WalletRepository
	txs     *apptest.WagerTransactionRepository
	ledgers *apptest.LedgerRepository
	outbox  *apptest.OutboxRepository
}

func newHarness() *harness {
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	inbox := apptest.NewInboxRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	svc := processwagertransaction.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, inbox, outbox, clock, ids)
	return &harness{svc: svc, wallets: wallets, txs: txs, ledgers: ledgers, outbox: outbox}
}

func (h *harness) seedWallet(t *testing.T, balance string) uuid.UUID {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, balance),
		Now:            time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, h.wallets.Insert(context.Background(), w))
	return w.ID()
}

func baseRequest(walletID uuid.UUID, kind wagertransaction.Kind, amount string) processwagertransaction.Request {
	return processwagertransaction.Request{
		IdempotencyKey:        "provider-a:" + uuid.NewString(),
		ProviderID:            "provider-a",
		ExternalTransactionID: uuid.NewString(),
		PayloadHash:           "hash-" + uuid.NewString(),
		WalletID:              walletID,
		PlayerID:              uuid.New(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Amount:                money.Money{},
		CorrelationID:         "corr-1",
	}
}

func TestHandle_Bet_DebitsWallet(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, result.Status)
	require.Equal(t, "75.00", result.Balance.String())

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "75.00", w.Balance().String())
}

func TestHandle_Bet_InsufficientBalance_Rejected(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "50.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "80.00")
	req.Amount = mustMoney(t, "80.00")

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, result.Status)
	require.Equal(t, processwagertransaction.FailureCodeInsufficientBalance, result.FailureCode)
	require.Equal(t, "50.00", result.Balance.String(), "balance must be unchanged on rejection")

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "50.00", w.Balance().String())
}

func TestHandle_Win_CreditsWallet(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindWin, "40.00")
	req.Amount = mustMoney(t, "40.00")

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "140.00", result.Balance.String())
}

func TestHandle_Loss_NoMovement_NoLedgerEntry(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindLoss, "0.00")
	req.Amount = mustMoney(t, "0.00")

	result, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, result.Status)
	require.Equal(t, "100.00", result.Balance.String())

	entries, _ := h.ledgers.ListByWallet(context.Background(), walletID)
	require.Empty(t, entries)

	for _, e := range h.outbox.Events {
		require.NotEqual(t, "WalletBalanceChanged", e.EventType, "LOSS must not emit WalletBalanceChanged")
	}
}

func TestHandle_IdempotentReplay_SameKeySameHash_ReturnsOriginalResult(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")

	first, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.False(t, first.IdempotentReplay)

	// wallet moves again after the original processing
	req2 := baseRequest(walletID, wagertransaction.KindWin, "1000.00")
	req2.Amount = mustMoney(t, "1000.00")
	_, err = h.svc.Handle(context.Background(), req2)
	require.NoError(t, err)

	replay, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.True(t, replay.IdempotentReplay)
	require.Equal(t, first.TransactionID, replay.TransactionID)
	require.Equal(t, first.Balance.String(), replay.Balance.String(), "replay must return the balance observed at original processing, not the current one")
}

func TestHandle_IdempotencyConflict_SameKeyDifferentPayload(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")
	_, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)

	req2 := req
	req2.PayloadHash = "different-hash"
	_, err = h.svc.Handle(context.Background(), req2)
	require.Error(t, err)
	require.True(t, errors.Is(err, processwagertransaction.ErrIdempotencyConflict))
}

func TestHandle_ExternalIDReusedWithDifferentKey_Rejected(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")
	_, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)

	req2 := req
	req2.IdempotencyKey = "a-completely-different-key"
	req2.PayloadHash = "different-hash-too"
	_, err = h.svc.Handle(context.Background(), req2)
	require.Error(t, err)
	require.True(t, errors.Is(err, processwagertransaction.ErrExternalIDReused))
}

func TestHandle_WalletNotFound(t *testing.T) {
	h := newHarness()
	req := baseRequest(uuid.New(), wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")

	_, err := h.svc.Handle(context.Background(), req)
	require.Error(t, err)
	require.True(t, errors.Is(err, processwagertransaction.ErrWalletNotFound))
}

func TestHandle_Refund_CreditsBackTheBetAmount(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	bet := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	bet.Amount = mustMoney(t, "25.00")
	betResult, err := h.svc.Handle(context.Background(), bet)
	require.NoError(t, err)
	require.Equal(t, "75.00", betResult.Balance.String())

	betTx, err := h.txs.GetByID(context.Background(), betResult.TransactionID)
	require.NoError(t, err)

	refund := baseRequest(walletID, wagertransaction.KindRefund, "25.00")
	refund.Amount = mustMoney(t, "25.00")
	refund.ReferenceExternalTransactionID = betTx.ExternalTransactionID()
	refund.PlayerID = betTx.PlayerID()

	refundResult, err := h.svc.Handle(context.Background(), refund)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, refundResult.Status)
	require.Equal(t, "100.00", refundResult.Balance.String())
}

func TestHandle_Refund_ReferenceMustBeABet(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	win := baseRequest(walletID, wagertransaction.KindWin, "25.00")
	win.Amount = mustMoney(t, "25.00")
	winResult, err := h.svc.Handle(context.Background(), win)
	require.NoError(t, err)
	winTx, err := h.txs.GetByID(context.Background(), winResult.TransactionID)
	require.NoError(t, err)

	refund := baseRequest(walletID, wagertransaction.KindRefund, "25.00")
	refund.Amount = mustMoney(t, "25.00")
	refund.ReferenceExternalTransactionID = winTx.ExternalTransactionID()
	refund.PlayerID = winTx.PlayerID()

	result, err := h.svc.Handle(context.Background(), refund)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, result.Status)
	require.Equal(t, processwagertransaction.FailureCodeInvalidReferenceKind, result.FailureCode)
}

func TestHandle_Refund_ReferenceNotFound_MarksPendingReference(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	refund := baseRequest(walletID, wagertransaction.KindRefund, "25.00")
	refund.Amount = mustMoney(t, "25.00")
	refund.ReferenceExternalTransactionID = "does-not-exist"

	result, err := h.svc.Handle(context.Background(), refund)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusPendingReference, result.Status)

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "100.00", w.Balance().String(), "wallet must not move while reference is pending")
}

func TestHandle_Refund_AmountMustMatchReference(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	bet := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	bet.Amount = mustMoney(t, "25.00")
	betResult, err := h.svc.Handle(context.Background(), bet)
	require.NoError(t, err)
	betTx, err := h.txs.GetByID(context.Background(), betResult.TransactionID)
	require.NoError(t, err)

	refund := baseRequest(walletID, wagertransaction.KindRefund, "10.00")
	refund.Amount = mustMoney(t, "10.00") // must equal the referenced BET's 25.00
	refund.ReferenceExternalTransactionID = betTx.ExternalTransactionID()
	refund.PlayerID = betTx.PlayerID()

	result, err := h.svc.Handle(context.Background(), refund)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, result.Status)
	require.Equal(t, processwagertransaction.FailureCodeReferenceAmountMismatch, result.FailureCode)
}

func TestHandle_Refund_DoubleReversal_SecondOneRejected(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	bet := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	bet.Amount = mustMoney(t, "25.00")
	betResult, err := h.svc.Handle(context.Background(), bet)
	require.NoError(t, err)
	betTx, err := h.txs.GetByID(context.Background(), betResult.TransactionID)
	require.NoError(t, err)

	refund1 := baseRequest(walletID, wagertransaction.KindRefund, "25.00")
	refund1.Amount = mustMoney(t, "25.00")
	refund1.ReferenceExternalTransactionID = betTx.ExternalTransactionID()
	refund1.PlayerID = betTx.PlayerID()
	result1, err := h.svc.Handle(context.Background(), refund1)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, result1.Status)

	refund2 := baseRequest(walletID, wagertransaction.KindRefund, "25.00")
	refund2.Amount = mustMoney(t, "25.00")
	refund2.ReferenceExternalTransactionID = betTx.ExternalTransactionID()
	refund2.PlayerID = betTx.PlayerID()
	result2, err := h.svc.Handle(context.Background(), refund2)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, result2.Status)
	require.Equal(t, processwagertransaction.FailureCodeReversalAlreadyProcessed, result2.FailureCode)
}

func TestHandle_Rollback_UndoesABet(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	bet := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	bet.Amount = mustMoney(t, "25.00")
	betResult, err := h.svc.Handle(context.Background(), bet)
	require.NoError(t, err)
	require.Equal(t, "75.00", betResult.Balance.String())
	betTx, err := h.txs.GetByID(context.Background(), betResult.TransactionID)
	require.NoError(t, err)

	rollback := baseRequest(walletID, wagertransaction.KindRollback, "25.00")
	rollback.Amount = mustMoney(t, "25.00")
	rollback.ReferenceExternalTransactionID = betTx.ExternalTransactionID()
	rollback.PlayerID = betTx.PlayerID()

	result, err := h.svc.Handle(context.Background(), rollback)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, result.Status)
	require.Equal(t, "100.00", result.Balance.String())
}

func TestHandle_Rollback_UndoesAWin(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	win := baseRequest(walletID, wagertransaction.KindWin, "40.00")
	win.Amount = mustMoney(t, "40.00")
	winResult, err := h.svc.Handle(context.Background(), win)
	require.NoError(t, err)
	require.Equal(t, "140.00", winResult.Balance.String())
	winTx, err := h.txs.GetByID(context.Background(), winResult.TransactionID)
	require.NoError(t, err)

	rollback := baseRequest(walletID, wagertransaction.KindRollback, "40.00")
	rollback.Amount = mustMoney(t, "40.00")
	rollback.ReferenceExternalTransactionID = winTx.ExternalTransactionID()
	rollback.PlayerID = winTx.PlayerID()

	result, err := h.svc.Handle(context.Background(), rollback)
	require.NoError(t, err)
	require.Equal(t, "100.00", result.Balance.String())
}

func TestHandle_Rollback_InsufficientBalance_UsesDistinctFailureCode(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	win := baseRequest(walletID, wagertransaction.KindWin, "40.00")
	win.Amount = mustMoney(t, "40.00")
	winResult, err := h.svc.Handle(context.Background(), win)
	require.NoError(t, err)
	winTx, err := h.txs.GetByID(context.Background(), winResult.TransactionID)
	require.NoError(t, err)

	// drain the wallet below the amount ROLLBACK would need to debit back
	bet := baseRequest(walletID, wagertransaction.KindBet, "130.00")
	bet.Amount = mustMoney(t, "130.00")
	_, err = h.svc.Handle(context.Background(), bet) // 140.00 - 130.00 = 10.00, less than the 40.00 WIN to undo
	require.NoError(t, err)

	rollback := baseRequest(walletID, wagertransaction.KindRollback, "40.00")
	rollback.Amount = mustMoney(t, "40.00")
	rollback.ReferenceExternalTransactionID = winTx.ExternalTransactionID()
	rollback.PlayerID = winTx.PlayerID()

	result, err := h.svc.Handle(context.Background(), rollback)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, result.Status)
	require.Equal(t, processwagertransaction.FailureCodeRollbackInsufficientBalance, result.FailureCode)
}

func TestHandle_InboxDeduplication_SameMessageIDTwice(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")
	req.Inbox = &processwagertransaction.InboxInfo{
		ConsumerName: "wager-consumer",
		MessageID:    "msg-1",
		Hash:         "hash-1",
	}

	first, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.False(t, first.IdempotentReplay)

	second, err := h.svc.Handle(context.Background(), req)
	require.NoError(t, err)
	require.True(t, second.IdempotentReplay)
	require.Equal(t, first.TransactionID, second.TransactionID)

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "75.00", w.Balance().String(), "the message must only ever move the wallet once")
}

// TestHandle_ConcurrentBets_MandatoryDisputeScenario is the challenge's
// required concurrency test: a wallet with 100.00 BRL receives two
// concurrent 80.00 BRL bets. Exactly one must be processed, the other
// rejected for insufficient balance, the final balance must be 20.00, and
// there must be exactly one ledger debit. NoopUnitOfWork has no real
// isolation, so this test exercises the aggregate's own Debit invariant
// under a shared in-memory wallet guarded by GetForUpdate's mutex — full
// cross-process proof comes with the Postgres adapter (Fase 4) and its
// SELECT ... FOR UPDATE.
func TestHandle_ConcurrentBets_MandatoryDisputeScenario(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "100.00")

	requests := make([]processwagertransaction.Request, 2)
	for i := range requests {
		req := baseRequest(walletID, wagertransaction.KindBet, "80.00")
		req.Amount = mustMoney(t, "80.00")
		requests[i] = req
	}

	results := make([]processwagertransaction.Result, 2)
	errs := make([]error, 2)

	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = h.svc.Handle(context.Background(), requests[i])
		}(i)
	}
	wg.Wait()

	for i := range errs {
		require.NoError(t, errs[i])
	}

	processedCount, rejectedCount := 0, 0
	for _, r := range results {
		switch r.Status {
		case wagertransaction.StatusProcessed:
			processedCount++
		case wagertransaction.StatusRejected:
			rejectedCount++
			require.Equal(t, processwagertransaction.FailureCodeInsufficientBalance, r.FailureCode)
		}
	}
	require.Equal(t, 1, processedCount, "exactly one bet must be processed")
	require.Equal(t, 1, rejectedCount, "exactly one bet must be rejected")

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "20.00", w.Balance().String())

	entries, err := h.ledgers.ListByWallet(context.Background(), walletID)
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one ledger debit")
	require.Equal(t, "80.00", entries[0].Amount().String())
}

// TestHandle_SameBetSentFiftyTimesInParallel_SingleDebit is the challenge's
// other required concurrency test: the same bet submitted 50 times
// concurrently must produce exactly one debit. All 50 calls share the same
// idempotency key, so 49 of them race to lose either the pre-check lookup
// or the INSERT itself (caught via ports.ErrAlreadyExists and turned into a
// replay, see Handle).
//
// NoopUnitOfWork has no real transaction isolation (unlike real Postgres,
// where a row is only ever visible to other transactions once fully
// committed — by which point it is already PROCESSED, never a bare
// PENDING), so a losing goroutine here can legitimately observe the
// winner's row mid-flight and report an intermediate status. What must
// hold regardless — and is asserted below — is the single financial
// outcome: one ledger entry, one final balance, every response naming the
// same transaction. The strict per-call PROCESSED guarantee is proven
// against real Postgres transactions in the adapter integration test.
func TestHandle_SameBetSentFiftyTimesInParallel_SingleDebit(t *testing.T) {
	h := newHarness()
	walletID := h.seedWallet(t, "1000.00")

	req := baseRequest(walletID, wagertransaction.KindBet, "25.00")
	req.Amount = mustMoney(t, "25.00")

	const attempts = 50
	results := make([]processwagertransaction.Result, attempts)
	errs := make([]error, attempts)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = h.svc.Handle(context.Background(), req)
		}(i)
	}
	wg.Wait()

	firstTransactionID := results[0].TransactionID
	for i := range results {
		require.NoError(t, errs[i])
		require.Equal(t, firstTransactionID, results[i].TransactionID, "all 50 calls must resolve to the same transaction")
	}

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	require.Equal(t, "975.00", w.Balance().String())

	entries, err := h.ledgers.ListByWallet(context.Background(), walletID)
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one debit for 50 identical concurrent submissions")
}
