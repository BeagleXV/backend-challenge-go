package referenceworker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

type testHarness struct {
	worker    *Worker
	wallets   *apptest.WalletRepository
	txs       *apptest.WagerTransactionRepository
	processor *processwagertransaction.Service
	nowFn     func() time.Time
}

func newTestHarness(t *testing.T, cfg Config) *testHarness {
	t.Helper()
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	inbox := apptest.NewInboxRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	processor := processwagertransaction.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, inbox, outbox, clock, ids)
	resolver := resolvependingreference.New(txs, processor)

	if cfg.Now == nil {
		cfg.Now = func() time.Time { return clock.T }
	}
	worker := New(resolver, cfg, metrics.NewNoop(), zaptest.NewLogger(t))

	return &testHarness{worker: worker, wallets: wallets, txs: txs, processor: processor, nowFn: cfg.Now}
}

func (h *testHarness) seedWallet(t *testing.T, balance string) uuid.UUID {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID: uuid.New(), PlayerID: uuid.New(), Currency: money.BRL,
		InitialBalance: mustMoney(t, balance), Now: time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, h.wallets.Insert(context.Background(), w))
	return w.ID()
}

func (h *testHarness) submitPendingRefund(t *testing.T, walletID uuid.UUID) uuid.UUID {
	t.Helper()
	req := processwagertransaction.Request{
		IdempotencyKey:                 "provider-a:refund-1",
		ProviderID:                     "provider-a",
		ExternalTransactionID:          "refund-1",
		WalletID:                       walletID,
		PlayerID:                       uuid.New(),
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           wagertransaction.KindRefund,
		Amount:                         mustMoney(t, "25.00"),
		ReferenceExternalTransactionID: "bet-does-not-exist",
	}
	result, err := h.processor.Handle(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusPendingReference, result.Status)
	return result.TransactionID
}

func TestRunOnce_NoCandidatesReady_DoesNothing(t *testing.T) {
	h := newTestHarness(t, Config{})
	h.worker.runOnce(context.Background())
	// No panics, no candidates: nothing to assert beyond "it didn't blow up".
}

func TestRunOnce_CandidateNotYetDue_Skipped(t *testing.T) {
	h := newTestHarness(t, Config{})
	walletID := h.seedWallet(t, "100.00")
	txID := h.submitPendingRefund(t, walletID)

	// The clock hasn't advanced past the 30s backoff yet.
	h.worker.runOnce(context.Background())

	tx, err := h.txs.GetByID(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, 1, tx.PendingReferenceAttempts(), "an attempt not yet due must not be retried")
}

func TestRunOnce_CandidateDue_RetriesAndReschedules(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	h := newTestHarness(t, Config{Now: func() time.Time { return now }})

	walletID := h.seedWallet(t, "100.00")
	txID := h.submitPendingRefund(t, walletID)

	now = base.Add(31 * time.Second) // past the first 30s backoff
	h.worker.runOnce(context.Background())

	tx, err := h.txs.GetByID(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, wagertransaction.StatusPendingReference, tx.Status())
	assert.Equal(t, 2, tx.PendingReferenceAttempts(), "a due candidate must be retried, bumping attempts")
}

func TestRunOnce_MaxAttemptsExhausted_Expires(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	h := newTestHarness(t, Config{MaxAttempts: 1, Now: func() time.Time { return now }})

	walletID := h.seedWallet(t, "100.00")
	txID := h.submitPendingRefund(t, walletID)

	now = base.Add(time.Hour) // well past due
	h.worker.runOnce(context.Background())

	tx, err := h.txs.GetByID(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, wagertransaction.StatusRejected, tx.Status())
	assert.Equal(t, processwagertransaction.FailureCodeReferenceNotFound, tx.FailureCode())

	w, err := h.wallets.GetByID(context.Background(), walletID)
	require.NoError(t, err)
	assert.Equal(t, "100.00", w.Balance().String(), "expiry never moves the wallet")
}

func TestRunOnce_TTLExhausted_Expires(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	h := newTestHarness(t, Config{MaxAttempts: 100, TTL: time.Minute, Now: func() time.Time { return now }})

	walletID := h.seedWallet(t, "100.00")
	txID := h.submitPendingRefund(t, walletID)

	now = base.Add(2 * time.Minute) // past the 1-minute TTL, well under MaxAttempts
	h.worker.runOnce(context.Background())

	tx, err := h.txs.GetByID(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, wagertransaction.StatusRejected, tx.Status())
	assert.Equal(t, processwagertransaction.FailureCodeReferenceNotFound, tx.FailureCode())
}

func TestStart_Stop_GracefulShutdown(t *testing.T) {
	h := newTestHarness(t, Config{PollInterval: 10 * time.Millisecond})
	walletID := h.seedWallet(t, "100.00")
	_ = h.submitPendingRefund(t, walletID)

	h.worker.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, h.worker.Stop(ctx))
}
