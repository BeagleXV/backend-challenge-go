// Package referenceworker implements the pending-reference worker: a
// periodic poller that retries resolving REFUND/ROLLBACK operations parked
// in PENDING_REFERENCE, and gives up (REJECTED, with a stable failure
// code) once a transaction's retry budget — max attempts or TTL,
// whichever comes first — is exhausted. All resolution logic itself
// (finding the reference, validating it, applying the reversal,
// scheduling the next backoff, or forcibly expiring) lives in
// processwagertransaction.Service via resolvependingreference — this
// package only owns the polling loop and the attempts/TTL policy that
// decides which of those two operations to call for each candidate.
package referenceworker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// Config tunes the worker's polling and give-up policy. Zero values are
// replaced with the documented defaults by New.
type Config struct {
	// PollInterval is how often the worker checks for due candidates.
	// Default 5s.
	PollInterval time.Duration
	// BatchSize caps how many candidates one poll cycle claims. Default 20.
	BatchSize int
	// MaxAttempts is the maximum number of resolution attempts (the
	// initial one that discovered the reference was missing, plus every
	// worker retry) before a transaction is force-expired. Default 10 —
	// combined with the exponential backoff cap (30min), that's a soft
	// ceiling of several hours even if TTL alone wouldn't have caught it
	// yet.
	MaxAttempts int
	// TTL bounds how long, since the transaction's original creation, the
	// worker keeps retrying at all — whichever of MaxAttempts or TTL is
	// hit first wins. Default 24h.
	TTL time.Duration
	// Now returns the current time; overridable in tests for a
	// deterministic clock. Defaults to time.Now.
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 20
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	if c.TTL <= 0 {
		c.TTL = 24 * time.Hour
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Worker polls for PENDING_REFERENCE transactions whose next attempt is
// due and resolves or expires each one.
type Worker struct {
	resolver *resolvependingreference.Service
	cfg      Config
	metrics  *metrics.Metrics
	logger   *zap.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

func New(resolver *resolvependingreference.Service, cfg Config, m *metrics.Metrics, logger *zap.Logger) *Worker {
	return &Worker{resolver: resolver, cfg: cfg.withDefaults(), metrics: m, logger: logger}
}

// Start launches the poll loop in the background and returns immediately.
// Must only be called once.
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	go w.run(ctx)
}

// Stop signals the poll loop to stop immediately, then waits — bounded by
// ctx — for a poll cycle already in progress to finish. If ctx is done
// first, Stop returns anyway: whatever row that cycle was mid-transaction
// on simply rolls back (WithinTx's own guarantee) and stays
// PENDING_REFERENCE, ready to be picked up by this or another instance's
// next poll — the same "always safely resumable" property that makes
// PENDING_REFERENCE durable across restarts in the first place.
func (w *Worker) Stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		w.logger.Warn("referenceworker: shutdown grace period elapsed with a poll cycle still in flight")
	}
	return nil
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) {
	now := w.cfg.Now()
	candidates, err := w.resolver.ListReady(ctx, now, w.cfg.BatchSize)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Error("referenceworker: list ready candidates failed", zap.Error(err))
		return
	}

	for _, tx := range candidates {
		w.handleCandidate(ctx, now, tx)
	}
}

func (w *Worker) handleCandidate(ctx context.Context, now time.Time, tx *wagertransaction.WagerTransaction) {
	exhausted := tx.PendingReferenceAttempts() >= w.cfg.MaxAttempts || now.Sub(tx.CreatedAt()) >= w.cfg.TTL

	if exhausted {
		start := time.Now()
		result, err := w.resolver.Expire(ctx, tx.ID(), "")
		w.metrics.RecordProcessingDuration(ctx, metrics.TransportWorker, time.Since(start))
		if err != nil {
			w.recordRaceOrError(ctx, "expire", tx.ID(), err)
			return
		}
		w.metrics.RecordWagerTransaction(ctx, string(result.Status), metrics.TransportWorker, false)
		w.logger.Info("referenceworker: expired pending reference",
			zap.String("transactionId", result.TransactionID.String()),
			zap.String("walletId", tx.WalletID().String()),
			zap.String("providerId", tx.ProviderID()),
			zap.Int("attempts", tx.PendingReferenceAttempts()),
			zap.Duration("age", now.Sub(tx.CreatedAt())),
		)
		return
	}

	start := time.Now()
	result, err := w.resolver.Resolve(ctx, tx.ID(), "")
	w.metrics.RecordProcessingDuration(ctx, metrics.TransportWorker, time.Since(start))
	if err != nil {
		w.recordRaceOrError(ctx, "resolve", tx.ID(), err)
		return
	}
	w.metrics.RecordWagerTransaction(ctx, string(result.Status), metrics.TransportWorker, false)
	if result.Status == wagertransaction.StatusPendingReference {
		w.metrics.RecordRetry(ctx, metrics.RetryKindPendingReference)
	}
	w.logger.Info("referenceworker: attempted pending reference resolution",
		zap.String("transactionId", result.TransactionID.String()),
		zap.String("walletId", tx.WalletID().String()),
		zap.String("providerId", tx.ProviderID()),
		zap.String("status", string(result.Status)),
	)
}

// recordRaceOrError distinguishes the benign race — another instance (or a
// direct HTTP/SQS resume) already resolved this transaction between this
// poll listing it and this worker acting on it — from an actual failure.
// Resume/ExpirePendingReference both return ErrNotPendingReference in
// exactly that case, since GetForUpdate serializes against any concurrent
// attempt on the same row and re-validates status after acquiring the
// lock.
func (w *Worker) recordRaceOrError(ctx context.Context, op string, transactionID uuid.UUID, err error) {
	if errors.Is(err, processwagertransaction.ErrNotPendingReference) {
		w.metrics.RecordConcurrencyConflict(ctx, metrics.ConflictReasonPendingReferenceRace)
		w.logger.Debug("referenceworker: candidate already resolved by another attempt", zap.String("op", op), zap.String("transactionId", transactionID.String()))
		return
	}
	w.logger.Error("referenceworker: "+op+" failed", zap.String("transactionId", transactionID.String()), zap.Error(err))
}
