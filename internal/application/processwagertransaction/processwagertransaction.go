// Package processwagertransaction implements the single use case shared by
// the HTTP handler and the SQS consumer for every external wager operation
// (BET/WIN/LOSS/REFUND/ROLLBACK). Both entry points normalize their input
// into the same Request and call Handle — there is exactly one place where
// idempotency, reference resolution and the financial rule per kind are
// decided, so HTTP and SQS can never diverge in what they guarantee.
package processwagertransaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/domain/event"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

// Stable, documented failure codes. Every business rejection this use case
// produces uses one of these, never an ad-hoc string, and a different code
// is used for every distinct reason so a provider can tell them apart.
const (
	FailureCodeInsufficientBalance         = "INSUFFICIENT_BALANCE"
	FailureCodeRollbackInsufficientBalance = "ROLLBACK_INSUFFICIENT_BALANCE"
	FailureCodeReferenceNotFound           = "REFERENCE_NOT_FOUND"
	FailureCodeReferenceNotProcessed       = "REFERENCE_NOT_PROCESSED"
	FailureCodeReversalAlreadyProcessed    = "REVERSAL_ALREADY_PROCESSED"
	FailureCodeInvalidReferenceKind        = "INVALID_REFERENCE_KIND"
	FailureCodeReferenceMismatch           = "REFERENCE_MISMATCH"
	FailureCodeReferenceAmountMismatch     = "REFERENCE_AMOUNT_MISMATCH"
)

var (
	ErrInvalidRequest      = errors.New("processwagertransaction: invalid request")
	ErrIdempotencyConflict = errors.New("processwagertransaction: idempotency key reused with a different payload")
	ErrExternalIDReused    = errors.New("processwagertransaction: operation already exists under a different idempotency key")
	ErrWalletNotFound      = errors.New("processwagertransaction: wallet not found")
	ErrUnsupportedKind     = errors.New("processwagertransaction: unsupported kind")
	ErrNotPendingReference = errors.New("processwagertransaction: transaction is not PENDING_REFERENCE")
	// ErrInboxHashMismatch signals a redelivered message whose content
	// hash no longer matches what was recorded under the same messageID
	// on first delivery — a reused id with silently different content,
	// never a normal at-least-once redelivery. Callers (the SQS consumer)
	// must not treat this as a replay: log it and leave the message for
	// SQS's own redrive policy to route to the DLQ after exhausting
	// retries, the same as any other Handle error.
	ErrInboxHashMismatch = errors.New("processwagertransaction: redelivered message hash does not match the original")
)

// InboxInfo carries SQS message identity for durable deduplication. Left
// nil for HTTP-originated requests, which have no transport-level message
// to deduplicate (idempotency is handled entirely via IdempotencyKey).
type InboxInfo struct {
	ConsumerName string
	MessageID    string
	Hash         string
}

// Request is the input already normalized by the caller (HTTP handler or
// SQS consumer) — this use case does not know or care which one it was.
// There is deliberately no PayloadHash field: Handle computes it itself,
// via CanonicalHash, from Request's own business fields — a caller cannot
// supply a wrong or inconsistently-normalized hash, and HTTP and SQS are
// structurally unable to diverge in what they consider the same
// operation.
type Request struct {
	IdempotencyKey                 string
	ProviderID                     string
	ExternalTransactionID          string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           wagertransaction.Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
	CorrelationID                  string
	Inbox                          *InboxInfo
}

// Result is what both HTTP and SQS translate back into their own response
// shape.
type Result struct {
	TransactionID    uuid.UUID
	Status           wagertransaction.Status
	Balance          money.Money
	HasBalance       bool
	IdempotentReplay bool
	FailureCode      string
}

// Service is the shared processing engine. Construct one per process (it
// is stateless beyond its dependencies) and call Handle from both the HTTP
// handler and the SQS consumer.
type Service struct {
	uow     ports.UnitOfWork
	wallets ports.WalletRepository
	txs     ports.WagerTransactionRepository
	ledgers ports.LedgerRepository
	inbox   ports.InboxRepository
	outbox  ports.OutboxRepository
	clock   ports.Clock
	ids     ports.IDGenerator
}

func New(
	uow ports.UnitOfWork,
	wallets ports.WalletRepository,
	txs ports.WagerTransactionRepository,
	ledgers ports.LedgerRepository,
	inbox ports.InboxRepository,
	outbox ports.OutboxRepository,
	clock ports.Clock,
	ids ports.IDGenerator,
) *Service {
	return &Service{
		uow:     uow,
		wallets: wallets,
		txs:     txs,
		ledgers: ledgers,
		inbox:   inbox,
		outbox:  outbox,
		clock:   clock,
		ids:     ids,
	}
}

func validateRequest(req Request) error {
	if req.IdempotencyKey == "" {
		return fmt.Errorf("%w: idempotencyKey is required", ErrInvalidRequest)
	}
	if req.ProviderID == "" || req.ExternalTransactionID == "" {
		return fmt.Errorf("%w: providerID and externalTransactionID are required", ErrInvalidRequest)
	}
	return nil
}

// errInsertConflict signals that Insert lost a race against a concurrent
// identical submission. It must never be returned to a caller: once a
// Postgres statement fails on a constraint violation, that transaction is
// aborted and no further statement can run on it (including a lookup to
// recover) — the only correct move is to let WithinTx roll the poisoned
// transaction back and retry the whole operation fresh, in a brand new
// transaction, where the winner's row is now visible.
var errInsertConflict = errors.New("processwagertransaction: concurrent insert conflict")

// Handle processes one external wager operation end to end: idempotency
// checks, reference resolution when applicable, the financial rule for the
// operation's kind, and persistence of the transaction, ledger entry (when
// there is a movement), inbox record (when the request came from SQS) and
// outbox events — all inside a single UnitOfWork. On a concurrent-insert
// conflict it retries once, in a fresh transaction; a second conflict is
// not retried further, since Postgres only returns that error once the
// winning transaction has already committed, so the retry's own lookups
// are guaranteed to observe it.
func (s *Service) Handle(ctx context.Context, req Request) (Result, error) {
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}

	result, err := s.handleOnce(ctx, req)
	if errors.Is(err, errInsertConflict) {
		result, err = s.handleOnce(ctx, req)
	}
	return result, err
}

func (s *Service) handleOnce(ctx context.Context, req Request) (Result, error) {
	hash := CanonicalHash(req)

	var result Result
	err := s.uow.WithinTx(ctx, func(ctx context.Context) error {
		if req.Inbox != nil {
			alreadyExists, err := s.inbox.TryInsert(ctx, ports.InboxMessage{
				ConsumerName: req.Inbox.ConsumerName,
				MessageID:    req.Inbox.MessageID,
				Hash:         req.Inbox.Hash,
			})
			if err != nil {
				return fmt.Errorf("inbox insert: %w", err)
			}
			if alreadyExists {
				stored, err := s.inbox.Get(ctx, req.Inbox.ConsumerName, req.Inbox.MessageID)
				if err != nil {
					return fmt.Errorf("inbox get: %w", err)
				}
				if stored.Hash != req.Inbox.Hash {
					return ErrInboxHashMismatch
				}
				r, err := s.loadExistingResult(ctx, req.IdempotencyKey)
				if err != nil {
					return err
				}
				result = r
				return nil
			}
		}

		existing, err := s.txs.FindByIdempotencyKey(ctx, req.IdempotencyKey)
		if err != nil && !errors.Is(err, ports.ErrNotFound) {
			return fmt.Errorf("lookup by idempotency key: %w", err)
		}
		if existing != nil {
			if existing.PayloadHash() != hash {
				return ErrIdempotencyConflict
			}
			result = resultFromExisting(existing)
			result.IdempotentReplay = true
			return s.completeInbox(ctx, req.Inbox)
		}

		byExternalID, err := s.txs.FindByProviderAndExternalID(ctx, req.ProviderID, req.ExternalTransactionID)
		if err != nil && !errors.Is(err, ports.ErrNotFound) {
			return fmt.Errorf("lookup by provider/external id: %w", err)
		}
		if byExternalID != nil && byExternalID.IdempotencyKey() != req.IdempotencyKey {
			return ErrExternalIDReused
		}

		w, err := s.wallets.GetForUpdate(ctx, req.WalletID)
		if err != nil {
			if errors.Is(err, ports.ErrNotFound) {
				return ErrWalletNotFound
			}
			return fmt.Errorf("load wallet: %w", err)
		}

		tx, err := wagertransaction.NewExternal(wagertransaction.NewExternalParams{
			ID:                             s.ids.NewID(),
			ProviderID:                     req.ProviderID,
			ExternalTransactionID:          req.ExternalTransactionID,
			IdempotencyKey:                 req.IdempotencyKey,
			PayloadHash:                    hash,
			WalletID:                       req.WalletID,
			PlayerID:                       req.PlayerID,
			RoundID:                        req.RoundID,
			GameID:                         req.GameID,
			Kind:                           req.Kind,
			Amount:                         req.Amount,
			ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
			Now:                            s.clock.Now(),
		})
		if err != nil {
			return fmt.Errorf("construct wager transaction: %w", err)
		}
		if err := s.txs.Insert(ctx, tx); err != nil {
			if errors.Is(err, ports.ErrAlreadyExists) {
				// Lost a race against a concurrent identical submission
				// that committed between our lookup above and this
				// INSERT. Abort immediately — no further statement is
				// safe on this transaction — and let Handle retry fresh.
				return errInsertConflict
			}
			return fmt.Errorf("insert wager transaction: %w", err)
		}

		pc := &processCtx{wallet: w, tx: tx, correlationID: correlationIDOrFallback(req.CorrelationID, tx.ID())}
		r, err := s.process(ctx, pc)
		if err != nil {
			return err
		}
		result = r

		return s.completeInbox(ctx, req.Inbox)
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// Resume re-attempts resolution of a transaction currently in
// PENDING_REFERENCE. It reuses the exact same reference-resolution logic as
// Handle, so the pending-reference worker (Fase 10) can never diverge in
// what counts as "resolved" from the synchronous path.
func (s *Service) Resume(ctx context.Context, transactionID uuid.UUID, correlationID string) (Result, error) {
	var result Result
	err := s.uow.WithinTx(ctx, func(ctx context.Context) error {
		// GetForUpdate, not GetByID: two instances resuming the same
		// PENDING_REFERENCE transaction concurrently must serialize on
		// this row, or both could apply the reversal's movement.
		tx, err := s.txs.GetForUpdate(ctx, transactionID)
		if err != nil {
			return fmt.Errorf("load wager transaction: %w", err)
		}
		if tx.Status() != wagertransaction.StatusPendingReference {
			return fmt.Errorf("%w: %s is %s", ErrNotPendingReference, transactionID, tx.Status())
		}

		w, err := s.wallets.GetForUpdate(ctx, tx.WalletID())
		if err != nil {
			return fmt.Errorf("load wallet: %w", err)
		}

		pc := &processCtx{wallet: w, tx: tx, correlationID: correlationIDOrFallback(correlationID, tx.ID())}
		r, err := s.applyReversal(ctx, pc)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// processCtx bundles the state threaded through the internal processing
// pipeline for a single operation.
type processCtx struct {
	wallet        *wallet.Wallet
	tx            *wagertransaction.WagerTransaction
	correlationID string
}

// correlationIDOrFallback ensures outbox events always carry a
// correlationId, per the required envelope shape, without ever letting a
// caller's missing correlation id fail the underlying financial processing
// — it is observability metadata, not a business precondition.
func correlationIDOrFallback(correlationID string, transactionID uuid.UUID) string {
	if correlationID != "" {
		return correlationID
	}
	return transactionID.String()
}

func (s *Service) process(ctx context.Context, pc *processCtx) (Result, error) {
	switch pc.tx.Kind() {
	case wagertransaction.KindBet:
		return s.applyDebit(ctx, pc, FailureCodeInsufficientBalance)
	case wagertransaction.KindWin:
		return s.applyCredit(ctx, pc)
	case wagertransaction.KindLoss:
		return s.finalizeNoMovement(ctx, pc)
	case wagertransaction.KindRefund, wagertransaction.KindRollback:
		return s.applyReversal(ctx, pc)
	default:
		return Result{}, fmt.Errorf("%w: %s", ErrUnsupportedKind, pc.tx.Kind())
	}
}

func (s *Service) applyDebit(ctx context.Context, pc *processCtx, insufficientCode string) (Result, error) {
	now := s.clock.Now()
	before, after, err := pc.wallet.Debit(pc.tx.Amount(), now)
	if err != nil {
		if errors.Is(err, wallet.ErrInsufficientBalance) {
			return s.reject(ctx, pc, insufficientCode)
		}
		return Result{}, fmt.Errorf("debit: %w", err)
	}
	return s.commitMovement(ctx, pc, ledger.DirectionDebit, before, after)
}

func (s *Service) applyCredit(ctx context.Context, pc *processCtx) (Result, error) {
	now := s.clock.Now()
	before, after, err := pc.wallet.Credit(pc.tx.Amount(), now)
	if err != nil {
		return Result{}, fmt.Errorf("credit: %w", err)
	}
	return s.commitMovement(ctx, pc, ledger.DirectionCredit, before, after)
}

func (s *Service) finalizeNoMovement(ctx context.Context, pc *processCtx) (Result, error) {
	now := s.clock.Now()
	pc.tx.SetResultBalance(pc.wallet.Balance())
	if err := pc.tx.MarkProcessed(now); err != nil {
		return Result{}, err
	}
	if err := s.txs.Update(ctx, pc.tx); err != nil {
		return Result{}, fmt.Errorf("update wager transaction: %w", err)
	}
	if err := s.enqueueProcessedEvent(ctx, pc); err != nil {
		return Result{}, err
	}
	return Result{TransactionID: pc.tx.ID(), Status: pc.tx.Status(), Balance: pc.wallet.Balance(), HasBalance: true}, nil
}

func (s *Service) reject(ctx context.Context, pc *processCtx, failureCode string) (Result, error) {
	now := s.clock.Now()
	pc.tx.SetResultBalance(pc.wallet.Balance())
	if err := pc.tx.MarkRejected(failureCode, now); err != nil {
		return Result{}, err
	}
	if err := s.txs.Update(ctx, pc.tx); err != nil {
		return Result{}, fmt.Errorf("update wager transaction: %w", err)
	}
	if err := s.enqueueRejectedEvent(ctx, pc, failureCode); err != nil {
		return Result{}, err
	}
	return Result{TransactionID: pc.tx.ID(), Status: pc.tx.Status(), Balance: pc.wallet.Balance(), HasBalance: true, FailureCode: failureCode}, nil
}

func (s *Service) commitMovement(ctx context.Context, pc *processCtx, direction ledger.Direction, before, after money.Money) (Result, error) {
	now := s.clock.Now()

	entry, err := ledger.New(ledger.NewParams{
		ID:            s.ids.NewID(),
		WalletID:      pc.wallet.ID(),
		TransactionID: pc.tx.ID(),
		Direction:     direction,
		Amount:        pc.tx.Amount(),
		BalanceBefore: before,
		BalanceAfter:  after,
		CreatedAt:     now,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build ledger entry: %w", err)
	}
	if err := s.wallets.Update(ctx, pc.wallet); err != nil {
		return Result{}, fmt.Errorf("update wallet: %w", err)
	}
	if err := s.ledgers.Append(ctx, entry); err != nil {
		return Result{}, fmt.Errorf("append ledger entry: %w", err)
	}

	pc.tx.SetResultBalance(after)
	if err := pc.tx.MarkProcessed(now); err != nil {
		return Result{}, err
	}
	if err := s.txs.Update(ctx, pc.tx); err != nil {
		return Result{}, fmt.Errorf("update wager transaction: %w", err)
	}

	if err := s.enqueueProcessedEvent(ctx, pc); err != nil {
		return Result{}, err
	}
	if err := s.enqueueBalanceChangedEvent(ctx, pc, direction, before, after); err != nil {
		return Result{}, err
	}

	return Result{TransactionID: pc.tx.ID(), Status: pc.tx.Status(), Balance: after, HasBalance: true}, nil
}

// reversalDirection reports which movement a REFUND/ROLLBACK applies given
// the kind of the operation it references, and whether that combination is
// allowed at all. REFUND only ever references a BET (credit back). ROLLBACK
// undoes a BET (credit), or a WIN/REFUND (debit) — never a LOSS, OPENING or
// another ROLLBACK.
func reversalDirection(reversalKind, referencedKind wagertransaction.Kind) (ledger.Direction, bool) {
	switch reversalKind {
	case wagertransaction.KindRefund:
		if referencedKind == wagertransaction.KindBet {
			return ledger.DirectionCredit, true
		}
		return "", false
	case wagertransaction.KindRollback:
		switch referencedKind {
		case wagertransaction.KindBet:
			return ledger.DirectionCredit, true
		case wagertransaction.KindWin, wagertransaction.KindRefund:
			return ledger.DirectionDebit, true
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func validateReferenceAgreement(tx, referenced *wagertransaction.WagerTransaction) bool {
	return tx.WalletID() == referenced.WalletID() &&
		tx.PlayerID() == referenced.PlayerID() &&
		tx.RoundID() == referenced.RoundID() &&
		tx.Amount().Currency() == referenced.Amount().Currency()
}

func (s *Service) applyReversal(ctx context.Context, pc *processCtx) (Result, error) {
	referenced, err := s.txs.FindByProviderAndExternalID(ctx, pc.tx.ProviderID(), pc.tx.ReferenceExternalTransactionID())
	if err != nil && !errors.Is(err, ports.ErrNotFound) {
		return Result{}, fmt.Errorf("lookup reference: %w", err)
	}
	if referenced == nil {
		return s.markPendingReference(ctx, pc)
	}

	if !validateReferenceAgreement(pc.tx, referenced) {
		return s.reject(ctx, pc, FailureCodeReferenceMismatch)
	}

	switch referenced.Status() {
	case wagertransaction.StatusPending, wagertransaction.StatusPendingReference:
		return s.markPendingReference(ctx, pc)
	case wagertransaction.StatusRejected, wagertransaction.StatusFailed:
		return s.reject(ctx, pc, FailureCodeReferenceNotProcessed)
	case wagertransaction.StatusProcessed:
		// proceeds below
	default:
		return Result{}, fmt.Errorf("%w: unexpected reference status %s", ErrUnsupportedKind, referenced.Status())
	}

	direction, allowed := reversalDirection(pc.tx.Kind(), referenced.Kind())
	if !allowed {
		return s.reject(ctx, pc, FailureCodeInvalidReferenceKind)
	}
	if !pc.tx.Amount().Equals(referenced.Amount()) {
		return s.reject(ctx, pc, FailureCodeReferenceAmountMismatch)
	}

	alreadyReversed, err := s.txs.HasSuccessfulReversal(ctx, pc.tx.ProviderID(), pc.tx.ReferenceExternalTransactionID(), pc.tx.Kind())
	if err != nil {
		return Result{}, fmt.Errorf("check existing reversal: %w", err)
	}
	if alreadyReversed {
		return s.reject(ctx, pc, FailureCodeReversalAlreadyProcessed)
	}

	now := s.clock.Now()
	if err := pc.tx.ResolveReference(referenced.ID(), now); err != nil {
		return Result{}, err
	}

	var before, after money.Money
	if direction == ledger.DirectionCredit {
		before, after, err = pc.wallet.Credit(pc.tx.Amount(), now)
	} else {
		before, after, err = pc.wallet.Debit(pc.tx.Amount(), now)
	}
	if err != nil {
		if errors.Is(err, wallet.ErrInsufficientBalance) {
			return s.reject(ctx, pc, FailureCodeRollbackInsufficientBalance)
		}
		return Result{}, fmt.Errorf("apply reversal movement: %w", err)
	}

	return s.commitMovement(ctx, pc, direction, before, after)
}

func (s *Service) markPendingReference(ctx context.Context, pc *processCtx) (Result, error) {
	now := s.clock.Now()
	if err := pc.tx.MarkPendingReference(now); err != nil {
		return Result{}, err
	}
	if err := s.txs.Update(ctx, pc.tx); err != nil {
		return Result{}, fmt.Errorf("update wager transaction: %w", err)
	}
	if err := s.enqueuePendingReferenceEvent(ctx, pc); err != nil {
		return Result{}, err
	}
	return Result{TransactionID: pc.tx.ID(), Status: pc.tx.Status()}, nil
}

func (s *Service) completeInbox(ctx context.Context, inbox *InboxInfo) error {
	if inbox == nil {
		return nil
	}
	if err := s.inbox.MarkCompleted(ctx, inbox.ConsumerName, inbox.MessageID, s.clock.Now()); err != nil {
		return fmt.Errorf("mark inbox completed: %w", err)
	}
	return nil
}

func (s *Service) loadExistingResult(ctx context.Context, idempotencyKey string) (Result, error) {
	existing, err := s.txs.FindByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return Result{}, fmt.Errorf("lookup by idempotency key after inbox replay: %w", err)
	}
	result := resultFromExisting(existing)
	result.IdempotentReplay = true
	return result, nil
}

func resultFromExisting(tx *wagertransaction.WagerTransaction) Result {
	balance, hasBalance := tx.ResultBalance()
	return Result{
		TransactionID: tx.ID(),
		Status:        tx.Status(),
		Balance:       balance,
		HasBalance:    hasBalance,
		FailureCode:   tx.FailureCode(),
	}
}

func (s *Service) enqueue(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, occurredAt time.Time, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	if err := s.outbox.Enqueue(ctx, eventID, aggregateID, eventType, data, occurredAt); err != nil {
		return fmt.Errorf("enqueue %s: %w", eventType, err)
	}
	return nil
}

func (s *Service) enqueueProcessedEvent(ctx context.Context, pc *processCtx) error {
	now := s.clock.Now()
	evt, err := event.NewWagerTransactionProcessed(s.ids.NewID(), pc.tx.ID(), pc.correlationID, "", now, event.WagerTransactionProcessedData{
		TransactionID:         pc.tx.ID(),
		WalletID:              pc.tx.WalletID(),
		ProviderID:            pc.tx.ProviderID(),
		ExternalTransactionID: pc.tx.ExternalTransactionID(),
		Kind:                  string(pc.tx.Kind()),
		Amount:                pc.tx.Amount(),
	})
	if err != nil {
		return fmt.Errorf("build WagerTransactionProcessed: %w", err)
	}
	return s.enqueue(ctx, evt.ID, evt.AggregateID, string(evt.EventType), evt.OccurredAt, evt)
}

func (s *Service) enqueueRejectedEvent(ctx context.Context, pc *processCtx, failureCode string) error {
	now := s.clock.Now()
	evt, err := event.NewWagerTransactionRejected(s.ids.NewID(), pc.tx.ID(), pc.correlationID, "", now, event.WagerTransactionRejectedData{
		TransactionID:         pc.tx.ID(),
		ProviderID:            pc.tx.ProviderID(),
		ExternalTransactionID: pc.tx.ExternalTransactionID(),
		Kind:                  string(pc.tx.Kind()),
		FailureCode:           failureCode,
	})
	if err != nil {
		return fmt.Errorf("build WagerTransactionRejected: %w", err)
	}
	return s.enqueue(ctx, evt.ID, evt.AggregateID, string(evt.EventType), evt.OccurredAt, evt)
}

func (s *Service) enqueuePendingReferenceEvent(ctx context.Context, pc *processCtx) error {
	now := s.clock.Now()
	evt, err := event.NewWagerTransactionPendingReference(s.ids.NewID(), pc.tx.ID(), pc.correlationID, "", now, event.WagerTransactionPendingReferenceData{
		TransactionID:                  pc.tx.ID(),
		ProviderID:                     pc.tx.ProviderID(),
		ExternalTransactionID:          pc.tx.ExternalTransactionID(),
		Kind:                           string(pc.tx.Kind()),
		ReferenceExternalTransactionID: pc.tx.ReferenceExternalTransactionID(),
	})
	if err != nil {
		return fmt.Errorf("build WagerTransactionPendingReference: %w", err)
	}
	return s.enqueue(ctx, evt.ID, evt.AggregateID, string(evt.EventType), evt.OccurredAt, evt)
}

func (s *Service) enqueueBalanceChangedEvent(ctx context.Context, pc *processCtx, direction ledger.Direction, before, after money.Money) error {
	now := s.clock.Now()
	evt, err := event.NewWalletBalanceChanged(s.ids.NewID(), pc.wallet.ID(), pc.correlationID, "", now, event.WalletBalanceChangedData{
		WalletID:      pc.wallet.ID(),
		TransactionID: pc.tx.ID(),
		Direction:     direction,
		Amount:        pc.tx.Amount(),
		BalanceBefore: before,
		BalanceAfter:  after,
		WalletVersion: pc.wallet.Version(),
	})
	if err != nil {
		return fmt.Errorf("build WalletBalanceChanged: %w", err)
	}
	return s.enqueue(ctx, evt.ID, evt.AggregateID, string(evt.EventType), evt.OccurredAt, evt)
}
