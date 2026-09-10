// Package apptest provides in-memory fakes for every port in
// internal/application/ports, used to unit-test use cases without any real
// infrastructure (Postgres adapters land in Fase 4). Not for production use.
package apptest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

// FixedClock always returns the same instant, for deterministic tests.
type FixedClock struct {
	T time.Time
}

func (c FixedClock) Now() time.Time { return c.T }

// SequentialIDs returns deterministic, incrementing UUIDs (v4 shape not
// required — tests only need uniqueness and stability).
type SequentialIDs struct {
	mu   sync.Mutex
	next int
}

func (g *SequentialIDs) NewID() uuid.UUID {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", g.next))
}

// lockRegistryKey stores the per-transaction row-lock registry in context,
// so NoopUnitOfWork can release every row lock a use case acquired via
// GetForUpdate once fn returns — mirroring how a real row lock is held
// until COMMIT/ROLLBACK, not released the instant the SELECT returns.
type lockRegistryKey struct{}

type lockRegistry struct {
	mu    sync.Mutex
	locks []*sync.Mutex
}

func (r *lockRegistry) add(m *sync.Mutex) {
	r.mu.Lock()
	r.locks = append(r.locks, m)
	r.mu.Unlock()
}

func (r *lockRegistry) releaseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.locks {
		m.Unlock()
	}
	r.locks = nil
}

// NoopUnitOfWork runs fn directly — the in-memory repositories below are
// not backed by real SQL transactions, so there is nothing to begin/commit/
// rollback. What it does provide is the row-lock lifecycle: any lock a
// repository acquires via GetForUpdate during fn is held until fn returns,
// exactly as a real "SELECT ... FOR UPDATE" stays held until the
// transaction ends.
type NoopUnitOfWork struct{}

func (NoopUnitOfWork) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	reg := &lockRegistry{}
	ctx = context.WithValue(ctx, lockRegistryKey{}, reg)
	defer reg.releaseAll()
	return fn(ctx)
}

// WalletRepository is an in-memory ports.WalletRepository.
type WalletRepository struct {
	mu       sync.Mutex
	byID     map[uuid.UUID]*wallet.Wallet
	byIndex  map[string]uuid.UUID // "playerID|currency" -> walletID
	rowLocks map[uuid.UUID]*sync.Mutex
}

func NewWalletRepository() *WalletRepository {
	return &WalletRepository{
		byID:     make(map[uuid.UUID]*wallet.Wallet),
		byIndex:  make(map[string]uuid.UUID),
		rowLocks: make(map[uuid.UUID]*sync.Mutex),
	}
}

func walletIndexKey(playerID uuid.UUID, currency money.Currency) string {
	return playerID.String() + "|" + string(currency)
}

func (r *WalletRepository) rowLock(id uuid.UUID) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.rowLocks[id]
	if !ok {
		m = &sync.Mutex{}
		r.rowLocks[id] = m
	}
	return m
}

// GetForUpdate blocks until it can acquire the wallet's row lock, then
// holds it for the rest of the current UnitOfWork.WithinTx call — the same
// shape as a real "SELECT ... FOR UPDATE" held until commit.
func (r *WalletRepository) GetForUpdate(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error) {
	lock := r.rowLock(walletID)
	lock.Lock()
	reg, ok := ctx.Value(lockRegistryKey{}).(*lockRegistry)
	if !ok {
		// Misuse: GetForUpdate called outside UnitOfWork.WithinTx. Release
		// immediately rather than leaking a permanently held lock.
		lock.Unlock()
		return nil, fmt.Errorf("apptest: GetForUpdate called outside WithinTx")
	}
	reg.add(lock)
	return r.GetByID(ctx, walletID)
}

func (r *WalletRepository) GetByID(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.byID[walletID]
	if !ok {
		return nil, fmt.Errorf("wallet %s: %w", walletID, ports.ErrNotFound)
	}
	return cloneWallet(w), nil
}

func (r *WalletRepository) GetByPlayerAndCurrency(ctx context.Context, playerID uuid.UUID, currency money.Currency) (*wallet.Wallet, error) {
	r.mu.Lock()
	id, ok := r.byIndex[walletIndexKey(playerID, currency)]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("wallet for player %s currency %s: %w", playerID, currency, ports.ErrNotFound)
	}
	return r.GetByID(ctx, id)
}

func (r *WalletRepository) Insert(ctx context.Context, w *wallet.Wallet) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := walletIndexKey(w.PlayerID(), w.Currency())
	if _, exists := r.byIndex[key]; exists {
		return fmt.Errorf("wallet for player %s currency %s: %w", w.PlayerID(), w.Currency(), ports.ErrAlreadyExists)
	}
	r.byIndex[key] = w.ID()
	r.byID[w.ID()] = cloneWallet(w)
	return nil
}

func (r *WalletRepository) Update(ctx context.Context, w *wallet.Wallet) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[w.ID()]; !ok {
		return fmt.Errorf("wallet %s: %w", w.ID(), ports.ErrNotFound)
	}
	r.byID[w.ID()] = cloneWallet(w)
	return nil
}

func cloneWallet(w *wallet.Wallet) *wallet.Wallet {
	clone, err := wallet.Rehydrate(wallet.RehydrateParams{
		ID:        w.ID(),
		PlayerID:  w.PlayerID(),
		Currency:  w.Currency(),
		Balance:   w.Balance(),
		Version:   w.Version(),
		CreatedAt: w.CreatedAt(),
		UpdatedAt: w.UpdatedAt(),
	})
	if err != nil {
		panic(fmt.Sprintf("apptest: cloning a wallet that was already valid should never fail: %v", err))
	}
	return clone
}

// WagerTransactionRepository is an in-memory ports.WagerTransactionRepository.
type WagerTransactionRepository struct {
	mu             sync.Mutex
	byID           map[uuid.UUID]*wagertransaction.WagerTransaction
	byIdempotency  map[string]uuid.UUID
	byProviderExtl map[string]uuid.UUID // "providerID|externalTransactionID"
}

func NewWagerTransactionRepository() *WagerTransactionRepository {
	return &WagerTransactionRepository{
		byID:           make(map[uuid.UUID]*wagertransaction.WagerTransaction),
		byIdempotency:  make(map[string]uuid.UUID),
		byProviderExtl: make(map[string]uuid.UUID),
	}
}

func providerExtKey(providerID, externalID string) string {
	return providerID + "|" + externalID
}

func cloneWagerTransaction(tx *wagertransaction.WagerTransaction) *wagertransaction.WagerTransaction {
	var resultBalance *money.Money
	if balance, ok := tx.ResultBalance(); ok {
		resultBalance = &balance
	}
	clone, err := wagertransaction.Rehydrate(wagertransaction.RehydrateParams{
		ID:                             tx.ID(),
		Origin:                         tx.Origin(),
		Kind:                           tx.Kind(),
		Status:                         tx.Status(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		IdempotencyKey:                 tx.IdempotencyKey(),
		PayloadHash:                    tx.PayloadHash(),
		RoundID:                        tx.RoundID(),
		GameID:                         tx.GameID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		ResolvedReferenceID:            tx.ResolvedReferenceID(),
		FailureCode:                    tx.FailureCode(),
		WalletID:                       tx.WalletID(),
		PlayerID:                       tx.PlayerID(),
		Amount:                         tx.Amount(),
		ResultBalance:                  resultBalance,
		CreatedAt:                      tx.CreatedAt(),
		UpdatedAt:                      tx.UpdatedAt(),
	})
	if err != nil {
		panic(fmt.Sprintf("apptest: cloning a wager transaction that was already valid should never fail: %v", err))
	}
	return clone
}

func (r *WagerTransactionRepository) Insert(ctx context.Context, tx *wagertransaction.WagerTransaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[tx.ID()] = cloneWagerTransaction(tx)
	if tx.IdempotencyKey() != "" {
		r.byIdempotency[tx.IdempotencyKey()] = tx.ID()
	}
	if tx.ProviderID() != "" {
		r.byProviderExtl[providerExtKey(tx.ProviderID(), tx.ExternalTransactionID())] = tx.ID()
	}
	return nil
}

func (r *WagerTransactionRepository) Update(ctx context.Context, tx *wagertransaction.WagerTransaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[tx.ID()]; !ok {
		return fmt.Errorf("wager transaction %s: %w", tx.ID(), ports.ErrNotFound)
	}
	r.byID[tx.ID()] = cloneWagerTransaction(tx)
	return nil
}

func (r *WagerTransactionRepository) GetByID(ctx context.Context, id uuid.UUID) (*wagertransaction.WagerTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("wager transaction %s: %w", id, ports.ErrNotFound)
	}
	return cloneWagerTransaction(tx), nil
}

func (r *WagerTransactionRepository) FindByIdempotencyKey(ctx context.Context, key string) (*wagertransaction.WagerTransaction, error) {
	r.mu.Lock()
	id, ok := r.byIdempotency[key]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("wager transaction with idempotency key %q: %w", key, ports.ErrNotFound)
	}
	return r.GetByID(ctx, id)
}

func (r *WagerTransactionRepository) FindByProviderAndExternalID(ctx context.Context, providerID, externalTransactionID string) (*wagertransaction.WagerTransaction, error) {
	r.mu.Lock()
	id, ok := r.byProviderExtl[providerExtKey(providerID, externalTransactionID)]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("wager transaction for provider %q external id %q: %w", providerID, externalTransactionID, ports.ErrNotFound)
	}
	return r.GetByID(ctx, id)
}

func (r *WagerTransactionRepository) HasSuccessfulReversal(ctx context.Context, providerID, referenceExternalID string, kind wagertransaction.Kind) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tx := range r.byID {
		if tx.ProviderID() == providerID &&
			tx.ReferenceExternalTransactionID() == referenceExternalID &&
			tx.Kind() == kind &&
			tx.Status() == wagertransaction.StatusProcessed {
			return true, nil
		}
	}
	return false, nil
}

func (r *WagerTransactionRepository) ListPendingReferenceForUpdate(ctx context.Context, limit int) ([]*wagertransaction.WagerTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*wagertransaction.WagerTransaction
	for _, tx := range r.byID {
		if tx.Status() == wagertransaction.StatusPendingReference {
			out = append(out, cloneWagerTransaction(tx))
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// LedgerRepository is an in-memory ports.LedgerRepository.
type LedgerRepository struct {
	mu      sync.Mutex
	entries []*ledger.Entry
}

func NewLedgerRepository() *LedgerRepository {
	return &LedgerRepository{}
}

func (r *LedgerRepository) Append(ctx context.Context, entry *ledger.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *LedgerRepository) ListByWallet(ctx context.Context, walletID uuid.UUID) ([]*ledger.Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*ledger.Entry
	for _, e := range r.entries {
		if e.WalletID() == walletID {
			out = append(out, e)
		}
	}
	return out, nil
}

// InboxRepository is an in-memory ports.InboxRepository.
type InboxRepository struct {
	mu   sync.Mutex
	rows map[string]*inboxRow
}

type inboxRow struct {
	msg         ports.InboxMessage
	completedAt *time.Time
}

func NewInboxRepository() *InboxRepository {
	return &InboxRepository{rows: make(map[string]*inboxRow)}
}

func inboxKey(consumerName, messageID string) string {
	return consumerName + "|" + messageID
}

func (r *InboxRepository) TryInsert(ctx context.Context, msg ports.InboxMessage) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := inboxKey(msg.ConsumerName, msg.MessageID)
	if _, exists := r.rows[key]; exists {
		return true, nil
	}
	r.rows[key] = &inboxRow{msg: msg}
	return false, nil
}

func (r *InboxRepository) MarkCompleted(ctx context.Context, consumerName, messageID string, completedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.rows[inboxKey(consumerName, messageID)]
	if !ok {
		return fmt.Errorf("inbox message %s/%s: %w", consumerName, messageID, ports.ErrNotFound)
	}
	row.completedAt = &completedAt
	return nil
}

// OutboxEvent is a recorded call to OutboxRepository.Enqueue, exposed for
// test assertions.
type OutboxEvent struct {
	EventID     uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte
	OccurredAt  time.Time
}

// OutboxRepository is an in-memory ports.OutboxRepository.
type OutboxRepository struct {
	mu     sync.Mutex
	Events []OutboxEvent
}

func NewOutboxRepository() *OutboxRepository {
	return &OutboxRepository{}
}

func (r *OutboxRepository) Enqueue(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte, occurredAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, OutboxEvent{
		EventID:     eventID,
		AggregateID: aggregateID,
		EventType:   eventType,
		Payload:     payload,
		OccurredAt:  occurredAt,
	})
	return nil
}
