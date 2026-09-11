//go:build integration

// Adapter-level integration tests against a real, ephemeral PostgreSQL
// container (testcontainers-go) — not a mock. They prove what only a real
// database can: row-level locking across genuinely separate connections,
// constraint/trigger enforcement, and transaction isolation. The broader
// end-to-end scenarios (multi-instance, HTTP+SQS crossovers, restart
// recovery) belong to the dedicated integration test suite in a later
// phase; these are scoped to the adapters this phase introduces.
package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/postgres"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

// testDB starts a fresh Postgres container, applies every migration, and
// returns a pool against it. The container is torn down when the test
// (and any subtests sharing t) completes.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("wagering"),
		tcpostgres.WithUsername("wagering_app"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// golang-migrate's pgx/v5 driver registers under the "pgx5" URL scheme,
	// not "postgres" — same DSN, different scheme prefix.
	migrateDSN := "pgx5" + dsn[len("postgres"):]
	m, err := migrate.New("file://../../../migrations", migrateDSN)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = m.Close()
	})
	require.NoError(t, m.Up())

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func insertWallet(t *testing.T, ctx context.Context, uow *postgres.UnitOfWork, wallets *postgres.WalletRepository, balance string) uuid.UUID {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, balance),
		Now:            time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error {
		return wallets.Insert(ctx, w)
	}))
	return w.ID()
}

func TestWalletRepository_GetForUpdate_SerializesAcrossRealConnections(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	walletID := insertWallet(t, ctx, uow, wallets, "100.00")

	// Two goroutines, each with its own transaction against the same real
	// pool (so genuinely separate server-side connections/sessions), race
	// to debit 80.00 from a 100.00 wallet. Exactly one must succeed.
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = uow.WithinTx(ctx, func(ctx context.Context) error {
				w, err := wallets.GetForUpdate(ctx, walletID)
				if err != nil {
					return err
				}
				_, _, err = w.Debit(mustMoney(t, "80.00"), time.Now().UTC())
				if err != nil {
					return err
				}
				return wallets.Update(ctx, w)
			})
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, err := range results {
		if err == nil {
			successCount++
		}
	}
	require.Equal(t, 1, successCount, "exactly one of the two concurrent debits must succeed")

	final, err := wallets.GetByID(ctx, walletID)
	require.NoError(t, err)
	require.Equal(t, "20.00", final.Balance().String())
}

func TestWalletRepository_Insert_DuplicatePlayerCurrency_ReturnsAlreadyExists(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)

	playerID := uuid.New()
	first, err := wallet.New(wallet.NewParams{
		ID: uuid.New(), PlayerID: playerID, Currency: money.BRL,
		InitialBalance: mustMoney(t, "0.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return wallets.Insert(ctx, first) }))

	second, err := wallet.New(wallet.NewParams{
		ID: uuid.New(), PlayerID: playerID, Currency: money.BRL,
		InitialBalance: mustMoney(t, "0.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	err = uow.WithinTx(ctx, func(ctx context.Context) error { return wallets.Insert(ctx, second) })
	require.ErrorIs(t, err, ports.ErrAlreadyExists)
}

func TestLedgerRepository_ImmutableAtDatabaseLevel(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "100.00")

	openingTx, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID: uuid.New(), WalletID: walletID, PlayerID: uuid.New(), Amount: mustMoney(t, "100.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, openingTx) }))

	entry, err := ledger.New(ledger.NewParams{
		ID: uuid.New(), WalletID: walletID, TransactionID: openingTx.ID(),
		Direction: ledger.DirectionCredit, Amount: mustMoney(t, "100.00"),
		BalanceBefore: mustMoney(t, "0.00"), BalanceAfter: mustMoney(t, "100.00"),
		CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return ledgers.Append(ctx, entry) }))

	entries, err := ledgers.ListByWallet(ctx, walletID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "100.00", entries[0].Amount().String())

	// Direct UPDATE/DELETE against the table, bypassing the repository (and
	// so mapErr) entirely, must still be rejected by the trigger itself —
	// the raw driver error is asserted here on purpose, since this checks
	// the database's own enforcement, independent of anything this
	// package's error mapping does.
	_, err = pool.Exec(ctx, "UPDATE wallet_ledger_entries SET amount = 999 WHERE id = $1", entry.ID())
	require.Error(t, err)
	require.Contains(t, err.Error(), "append-only")

	_, err = pool.Exec(ctx, "DELETE FROM wallet_ledger_entries WHERE id = $1", entry.ID())
	require.Error(t, err)
}

// TestLedgerRepository_ListByWalletPage_KeysetPaginationOverRealRows proves
// the (created_at, id) keyset comparison the HTTP ledger endpoint (Fase 6)
// relies on actually works against Postgres's own ordering and tuple
// comparison — the in-memory fake's equivalent logic is exercised by unit
// tests, but only a real query can prove the SQL is correct.
func TestLedgerRepository_ListByWalletPage_KeysetPaginationOverRealRows(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "1000.00")

	openingTx, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID: uuid.New(), WalletID: walletID, PlayerID: uuid.New(), Amount: mustMoney(t, "1000.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, openingTx) }))

	// Five entries, each against its own BET transaction (the unique
	// constraint on (wallet_id, transaction_id) forbids reusing one) and
	// each carrying a distinct, increasing balance so their relative
	// order after paging is easy to assert.
	const total = 5
	before := mustMoney(t, "1000.00")
	for i := 0; i < total; i++ {
		betTx, err := wagertransaction.NewExternal(wagertransaction.NewExternalParams{
			ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: fmt.Sprintf("tx-%d", i),
			IdempotencyKey: fmt.Sprintf("provider-a:tx-%d", i), PayloadHash: fmt.Sprintf("hash-%d", i),
			WalletID: walletID, PlayerID: uuid.New(), RoundID: "round-1", GameID: "game-1",
			Kind: wagertransaction.KindBet, Amount: mustMoney(t, "100.00"), Now: time.Now().UTC(),
		})
		require.NoError(t, err)
		require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, betTx) }))

		after := mustMoney(t, fmt.Sprintf("%d.00", 1000-(i+1)*100))
		entry, err := ledger.New(ledger.NewParams{
			ID: uuid.New(), WalletID: walletID, TransactionID: betTx.ID(),
			Direction: ledger.DirectionDebit, Amount: mustMoney(t, "100.00"),
			BalanceBefore: before, BalanceAfter: after,
			CreatedAt: time.Now().UTC(),
		})
		require.NoError(t, err)
		require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return ledgers.Append(ctx, entry) }))
		before = after
	}

	var (
		afterCreatedAt time.Time
		afterID        uuid.UUID
		seen           []string
	)
	for {
		page, err := ledgers.ListByWalletPage(ctx, walletID, afterCreatedAt, afterID, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			seen = append(seen, e.BalanceAfter().String())
		}
		last := page[len(page)-1]
		afterCreatedAt = last.CreatedAt()
		afterID = last.ID()
	}

	require.Equal(t, []string{"900.00", "800.00", "700.00", "600.00", "500.00"}, seen)
}

func TestInboxRepository_ConcurrentSameMessage_OnlyOneWins(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	inbox := postgres.NewInboxRepository(pool)

	msg := ports.InboxMessage{ConsumerName: "wager-consumer", MessageID: "msg-1", Hash: "hash-1"}

	const attempts = 10
	alreadyExists := make([]bool, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = uow.WithinTx(ctx, func(ctx context.Context) error {
				var err error
				alreadyExists[i], err = inbox.TryInsert(ctx, msg)
				return err
			})
		}(i)
	}
	wg.Wait()

	newCount := 0
	for _, existed := range alreadyExists {
		if !existed {
			newCount++
		}
	}
	require.Equal(t, 1, newCount, "exactly one of the concurrent identical deliveries must be the first insert")
}

func TestOutboxRepository_ClaimBatch_TwoPublishersDoNotDoubleClaim(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	outbox := postgres.NewOutboxRepository(pool)

	const eventCount = 20
	for i := 0; i < eventCount; i++ {
		require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error {
			return outbox.Enqueue(ctx, uuid.New(), uuid.New(), "WagerTransactionProcessed", []byte(`{}`), time.Now().UTC())
		}))
	}

	var mu sync.Mutex
	claimedIDs := map[uuid.UUID]int{}

	var wg sync.WaitGroup
	for p := 0; p < 2; p++ {
		wg.Add(1)
		go func(publisher int) {
			defer wg.Done()
			lockedBy := fmt.Sprintf("publisher-%d", publisher)
			_ = uow.WithinTx(ctx, func(ctx context.Context) error {
				batch, err := outbox.ClaimBatch(ctx, eventCount, lockedBy, time.Minute)
				if err != nil {
					return err
				}
				mu.Lock()
				for _, rec := range batch {
					claimedIDs[rec.EventID]++
				}
				mu.Unlock()
				return nil
			})
		}(p)
	}
	wg.Wait()

	require.Len(t, claimedIDs, eventCount, "every event must be claimed by exactly one publisher")
	for id, count := range claimedIDs {
		require.Equal(t, 1, count, "event %s claimed more than once", id)
	}
}

func TestWagerTransactionRepository_HasSuccessfulReversal(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "100.00")
	playerID := uuid.New()

	bet, err := wagertransaction.NewExternal(wagertransaction.NewExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: "bet-1",
		IdempotencyKey: "provider-a:bet-1", PayloadHash: "hash-1",
		WalletID: walletID, PlayerID: playerID, RoundID: "round-1", GameID: "game-1",
		Kind: wagertransaction.KindBet, Amount: mustMoney(t, "25.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, bet) }))

	has, err := txs.HasSuccessfulReversal(ctx, "provider-a", "bet-1", wagertransaction.KindRefund)
	require.NoError(t, err)
	require.False(t, has)

	refund, err := wagertransaction.NewExternal(wagertransaction.NewExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: "refund-1",
		IdempotencyKey: "provider-a:refund-1", PayloadHash: "hash-2",
		WalletID: walletID, PlayerID: playerID, RoundID: "round-1", GameID: "game-1",
		Kind: wagertransaction.KindRefund, Amount: mustMoney(t, "25.00"),
		ReferenceExternalTransactionID: "bet-1", Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, refund.MarkProcessed(time.Now().UTC()))
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, refund) }))

	has, err = txs.HasSuccessfulReversal(ctx, "provider-a", "bet-1", wagertransaction.KindRefund)
	require.NoError(t, err)
	require.True(t, has)
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type uuidGenerator struct{}

func (uuidGenerator) NewID() uuid.UUID { return uuid.New() }

// TestProcessWagerTransaction_SameBetSentFiftyTimesInParallel_SingleDebit
// wires processwagertransaction.Service to the real Postgres adapters end
// to end and proves the strict guarantee the in-memory fake could not: with
// real transaction isolation, every one of the 50 concurrent identical
// submissions observes a fully committed PROCESSED result (never an
// intermediate PENDING), because a losing transaction can only ever see the
// winner's row once the winner's entire transaction — insert through
// PROCESSED — has committed atomically.
func TestProcessWagerTransaction_SameBetSentFiftyTimesInParallel_SingleDebit(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)
	inbox := postgres.NewInboxRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)

	svc := processwagertransaction.New(uow, wallets, txs, ledgers, inbox, outbox, systemClock{}, uuidGenerator{})

	walletID := insertWallet(t, ctx, uow, wallets, "1000.00")

	req := processwagertransaction.Request{
		IdempotencyKey:        "provider-a:bet-parallel-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-parallel-1",
		WalletID:              walletID,
		PlayerID:              uuid.New(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  wagertransaction.KindBet,
		Amount:                mustMoney(t, "25.00"),
		CorrelationID:         "corr-1",
	}

	const attempts = 50
	results := make([]processwagertransaction.Result, attempts)
	errs := make([]error, attempts)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.Handle(ctx, req)
		}(i)
	}
	wg.Wait()

	firstTransactionID := results[0].TransactionID
	for i := range results {
		require.NoError(t, errs[i])
		require.Equal(t, wagertransaction.StatusProcessed, results[i].Status, "call %d", i)
		require.Equal(t, firstTransactionID, results[i].TransactionID, "call %d", i)
		require.Equal(t, "975.00", results[i].Balance.String(), "call %d", i)
	}

	w, err := wallets.GetByID(ctx, walletID)
	require.NoError(t, err)
	require.Equal(t, "975.00", w.Balance().String())

	entries, err := ledgers.ListByWallet(ctx, walletID)
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one debit for 50 identical concurrent submissions")
}

// TestProcessWagerTransaction_InboxAndOutboxCommitAtomicallyWithDomainChange
// is the Fase 8 proof, against a real Postgres transaction rather than the
// in-memory fakes: an SQS-originated BET's inbox record, wallet debit,
// ledger entry and outbox events all land in the database as one commit.
// Querying the pool directly afterwards (not through the application's own
// UnitOfWork) confirms every one of them was actually durably committed,
// not merely visible to the same in-flight transaction.
func TestProcessWagerTransaction_InboxAndOutboxCommitAtomicallyWithDomainChange(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()

	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)
	inbox := postgres.NewInboxRepository(pool)
	outbox := postgres.NewOutboxRepository(pool)

	svc := processwagertransaction.New(uow, wallets, txs, ledgers, inbox, outbox, systemClock{}, uuidGenerator{})

	walletID := insertWallet(t, ctx, uow, wallets, "100.00")

	req := processwagertransaction.Request{
		IdempotencyKey:        "provider-a:bet-atomic-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-atomic-1",
		WalletID:              walletID,
		PlayerID:              uuid.New(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  wagertransaction.KindBet,
		Amount:                mustMoney(t, "25.00"),
		CorrelationID:         "corr-1",
		Inbox: &processwagertransaction.InboxInfo{
			ConsumerName: "wager-consumer",
			MessageID:    "sqs-msg-atomic-1",
			Hash:         "hash-atomic-1",
		},
	}

	result, err := svc.Handle(ctx, req)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, result.Status)

	// Domain change: wallet debited, exactly one ledger entry.
	w, err := wallets.GetByID(ctx, walletID)
	require.NoError(t, err)
	require.Equal(t, "75.00", w.Balance().String())
	entries, err := ledgers.ListByWallet(ctx, walletID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Inbox: the exact same message, redelivered, is recognized as already
	// handled — proving the row was committed, not rolled back with
	// everything else it shares a transaction with.
	alreadyExists, err := inbox.TryInsert(ctx, ports.InboxMessage{
		ConsumerName: "wager-consumer", MessageID: "sqs-msg-atomic-1", Hash: "hash-atomic-1",
	})
	require.NoError(t, err)
	require.True(t, alreadyExists)

	// Outbox: both events this operation must produce (README section 11)
	// are present, queued for the Fase 11 publisher — never published
	// inline by this call, only ever written to this table.
	var eventTypes []string
	rows, err := pool.Query(ctx, `SELECT event_type FROM outbox_events WHERE aggregate_id = $1 OR aggregate_id = $2 ORDER BY event_type`, result.TransactionID, walletID)
	require.NoError(t, err)
	for rows.Next() {
		var eventType string
		require.NoError(t, rows.Scan(&eventType))
		eventTypes = append(eventTypes, eventType)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []string{"WagerTransactionProcessed", "WalletBalanceChanged"}, eventTypes)

	var publishedCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL`).Scan(&publishedCount))
	require.Zero(t, publishedCount, "nothing publishes an outbox row inline — that's the Fase 11 worker's job")
}

// TestWagerTransactionRepository_PendingReferenceRetryLifecycle proves,
// against real Postgres, the parts of the Fase 10 retry/expiry bookkeeping
// that only a real database can: the schema's own CHECK constraint
// (status = 'PENDING_REFERENCE') = (pending_reference_next_attempt_at IS
// NOT NULL) actually holds — in particular that transitioning a pending
// transaction to REJECTED without clearing next_attempt_at would be
// rejected by Postgres itself, not just assumed correct because the
// domain layer's own transitionTo happens to clear it — and that
// ListPendingReferenceForUpdate's real query filters by the due time
// exactly as intended.
func TestWagerTransactionRepository_PendingReferenceRetryLifecycle(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "100.00")
	now := time.Now().UTC()

	refund, err := wagertransaction.NewExternal(wagertransaction.NewExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: "refund-1",
		IdempotencyKey: "provider-a:refund-1", PayloadHash: "hash-1",
		WalletID: walletID, PlayerID: uuid.New(), RoundID: "round-1", GameID: "game-1",
		Kind: wagertransaction.KindRefund, Amount: mustMoney(t, "25.00"),
		ReferenceExternalTransactionID: "bet-does-not-exist", Now: now,
	})
	require.NoError(t, err)
	require.NoError(t, refund.MarkPendingReference(now, now.Add(30*time.Second)))
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, refund) }))

	// Not due yet.
	ready, err := txs.ListPendingReferenceForUpdate(ctx, now.Add(10*time.Second), 10)
	require.NoError(t, err)
	require.Empty(t, ready)

	// Due: the real query returns it, and the round trip preserved
	// attempts/next_attempt_at exactly.
	ready, err = txs.ListPendingReferenceForUpdate(ctx, now.Add(31*time.Second), 10)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	require.Equal(t, refund.ID(), ready[0].ID())
	require.Equal(t, 1, ready[0].PendingReferenceAttempts())
	gotNext, ok := ready[0].PendingReferenceNextAttemptAt()
	require.True(t, ok)
	require.WithinDuration(t, now.Add(30*time.Second), gotNext, time.Millisecond)

	// A retry persists correctly too.
	retryNow := now.Add(31 * time.Second)
	require.NoError(t, refund.RecordPendingReferenceRetry(retryNow, retryNow.Add(60*time.Second)))
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Update(ctx, refund) }))

	reloaded, err := txs.GetByID(ctx, refund.ID())
	require.NoError(t, err)
	require.Equal(t, 2, reloaded.PendingReferenceAttempts())

	// Expiring it (REJECTED) must clear next_attempt_at — if the domain
	// layer failed to do that, this UPDATE would violate the CHECK
	// constraint and fail here, against real Postgres, regardless of what
	// the in-memory fake would have allowed.
	expireNow := retryNow.Add(90 * time.Second)
	w, err := wallets.GetByID(ctx, walletID)
	require.NoError(t, err)
	reloaded.SetResultBalance(w.Balance())
	require.NoError(t, reloaded.MarkRejected("REFERENCE_NOT_FOUND", expireNow))
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Update(ctx, reloaded) }))

	final, err := txs.GetByID(ctx, refund.ID())
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusRejected, final.Status())
	_, ok = final.PendingReferenceNextAttemptAt()
	require.False(t, ok)

	// Now terminal: no longer a candidate for the worker to pick up.
	ready, err = txs.ListPendingReferenceForUpdate(ctx, expireNow.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, ready)
}

// TestReconciliation_RepeatableRead_ConsistentSnapshotDespiteConcurrentWrite
// is the Fase 12 proof: reconciliation reads the wallet balance, then
// (later, as a separate statement) the full ledger. Under the default READ
// COMMITTED isolation, a BET committing in between those two reads would
// make an entirely healthy wallet look divergent — the balance read
// reflects the old state, the ledger read reflects the new one. This test
// drives exactly that interleaving by hand, through
// UnitOfWork.WithinRepeatableReadTx (what reconciliation.Service actually
// uses), and asserts the snapshot it computes is entirely
// pre-concurrent-write and self-consistent — never a spurious mismatch.
func TestReconciliation_RepeatableRead_ConsistentSnapshotDespiteConcurrentWrite(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "1000.00")

	opening, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID: uuid.New(), WalletID: walletID, PlayerID: uuid.New(), Amount: mustMoney(t, "1000.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, opening) }))

	openingEntry, err := ledger.New(ledger.NewParams{
		ID: uuid.New(), WalletID: walletID, TransactionID: opening.ID(),
		Direction: ledger.DirectionCredit, Amount: mustMoney(t, "1000.00"),
		BalanceBefore: mustMoney(t, "0.00"), BalanceAfter: mustMoney(t, "1000.00"),
		CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return ledgers.Append(ctx, openingEntry) }))

	type snapshot struct {
		balance    string
		ledgerSum  string
		entryCount int
	}
	readyForConcurrentWrite := make(chan struct{})
	continueSecondRead := make(chan struct{})
	resultCh := make(chan snapshot, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- uow.WithinRepeatableReadTx(ctx, func(ctx context.Context) error {
			w, err := wallets.GetByID(ctx, walletID) // first statement: pins the snapshot
			if err != nil {
				return err
			}
			close(readyForConcurrentWrite)
			<-continueSecondRead

			entries, err := ledgers.ListByWallet(ctx, walletID)
			if err != nil {
				return err
			}
			sum, err := money.Zero(money.BRL)
			if err != nil {
				return err
			}
			for _, e := range entries {
				switch e.Direction() {
				case ledger.DirectionCredit:
					sum, err = sum.Add(e.Amount())
				case ledger.DirectionDebit:
					sum, err = sum.Sub(e.Amount())
				}
				if err != nil {
					return err
				}
			}
			resultCh <- snapshot{balance: w.Balance().String(), ledgerSum: sum.String(), entryCount: len(entries)}
			return nil
		})
	}()

	<-readyForConcurrentWrite

	// The concurrent write: a real BET, processed end to end on its own
	// connection while the reconciliation transaction above sits between
	// its two reads.
	svc := processwagertransaction.New(
		postgres.NewUnitOfWork(pool), wallets, txs, ledgers,
		postgres.NewInboxRepository(pool), postgres.NewOutboxRepository(pool),
		systemClock{}, uuidGenerator{},
	)
	betResult, err := svc.Handle(ctx, processwagertransaction.Request{
		IdempotencyKey: "provider-a:bet-concurrent-1", ProviderID: "provider-a", ExternalTransactionID: "bet-concurrent-1",
		WalletID: walletID, PlayerID: uuid.New(), RoundID: "round-1", GameID: "game-1",
		Kind: wagertransaction.KindBet, Amount: mustMoney(t, "100.00"),
	})
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, betResult.Status)

	close(continueSecondRead)
	require.NoError(t, <-errCh)
	snap := <-resultCh

	// Both reads the reconciliation transaction made came from strictly
	// before the concurrent BET — self-consistent, never a spurious
	// mismatch caused purely by timing.
	require.Equal(t, "1000.00", snap.balance)
	require.Equal(t, "1000.00", snap.ledgerSum)
	require.Equal(t, 1, snap.entryCount)

	// The concurrent write really did happen — an ordinary read (no
	// transaction, no snapshot) sees it.
	w, err := wallets.GetByID(ctx, walletID)
	require.NoError(t, err)
	require.Equal(t, "900.00", w.Balance().String())
}

// TestReadCommitted_SameInterleaving_ProducesSpuriousDivergence is the
// negative control for the test above: the exact same interleaving, but
// through plain WithinTx (READ COMMITTED, what every other use case in
// this codebase correctly uses) instead of WithinRepeatableReadTx. It
// reproduces the bug reconciliation.Service would have if it used the
// wrong isolation level — the balance read observes the old value, the
// ledger read observes the new entry, and the two together look like a
// 100.00 divergence in a wallet that was never actually wrong. This is
// what justifies Reconcile's use of WithinRepeatableReadTx being load-
// bearing rather than a defensive nicety.
func TestReadCommitted_SameInterleaving_ProducesSpuriousDivergence(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	uow := postgres.NewUnitOfWork(pool)
	wallets := postgres.NewWalletRepository(pool)
	txs := postgres.NewWagerTransactionRepository(pool)
	ledgers := postgres.NewLedgerRepository(pool)

	walletID := insertWallet(t, ctx, uow, wallets, "1000.00")

	opening, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID: uuid.New(), WalletID: walletID, PlayerID: uuid.New(), Amount: mustMoney(t, "1000.00"), Now: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return txs.Insert(ctx, opening) }))

	openingEntry, err := ledger.New(ledger.NewParams{
		ID: uuid.New(), WalletID: walletID, TransactionID: opening.ID(),
		Direction: ledger.DirectionCredit, Amount: mustMoney(t, "1000.00"),
		BalanceBefore: mustMoney(t, "0.00"), BalanceAfter: mustMoney(t, "1000.00"),
		CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, uow.WithinTx(ctx, func(ctx context.Context) error { return ledgers.Append(ctx, openingEntry) }))

	readyForConcurrentWrite := make(chan struct{})
	continueSecondRead := make(chan struct{})
	balanceCh := make(chan string, 1)
	entryCountCh := make(chan int, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- uow.WithinTx(ctx, func(ctx context.Context) error { // READ COMMITTED, on purpose
			w, err := wallets.GetByID(ctx, walletID)
			if err != nil {
				return err
			}
			balanceCh <- w.Balance().String()
			close(readyForConcurrentWrite)
			<-continueSecondRead

			entries, err := ledgers.ListByWallet(ctx, walletID)
			if err != nil {
				return err
			}
			entryCountCh <- len(entries)
			return nil
		})
	}()

	<-readyForConcurrentWrite

	svc := processwagertransaction.New(
		postgres.NewUnitOfWork(pool), wallets, txs, ledgers,
		postgres.NewInboxRepository(pool), postgres.NewOutboxRepository(pool),
		systemClock{}, uuidGenerator{},
	)
	betResult, err := svc.Handle(ctx, processwagertransaction.Request{
		IdempotencyKey: "provider-a:bet-concurrent-2", ProviderID: "provider-a", ExternalTransactionID: "bet-concurrent-2",
		WalletID: walletID, PlayerID: uuid.New(), RoundID: "round-1", GameID: "game-1",
		Kind: wagertransaction.KindBet, Amount: mustMoney(t, "100.00"),
	})
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, betResult.Status)

	close(continueSecondRead)
	require.NoError(t, <-errCh)

	balance := <-balanceCh
	entryCount := <-entryCountCh

	// The balance read is stale (pre-BET) but the ledger read is fresh
	// (post-BET) — under READ COMMITTED, each statement gets its own
	// snapshot. Comparing the two, as reconciliation does, would report a
	// 100.00 difference for a wallet that is, and always was, correct.
	require.Equal(t, "1000.00", balance, "read before the concurrent commit")
	require.Equal(t, 2, entryCount, "read after the concurrent commit — sees the new BET's ledger entry the balance read above does not know about")
}
