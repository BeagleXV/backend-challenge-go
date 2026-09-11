package outboxpublisher

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// Config tunes the worker's polling, batching and lock-recovery behavior.
// Zero values are replaced with the documented defaults by New.
type Config struct {
	// PollInterval is how often the worker checks for pending events.
	// Default 5s.
	PollInterval time.Duration
	// BatchSize caps how many events one poll cycle claims. Default 20.
	BatchSize int
	// LockDuration is how long a claim is honored before another
	// publisher instance (or this one, on a later poll) may reclaim the
	// same row. This is also, by construction, the retry interval after a
	// failed publish attempt: a row that fails to publish is simply left
	// claimed-but-unconfirmed, and becomes reclaimable once its lock goes
	// stale — no separate backoff bookkeeping is needed on top of it.
	// Must comfortably exceed how long one publish attempt normally
	// takes. Default 30s.
	LockDuration time.Duration
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 20
	}
	if c.LockDuration <= 0 {
		c.LockDuration = 30 * time.Second
	}
	return c
}

// Worker polls the outbox for unpublished, due events and publishes each
// one, supporting multiple concurrently-running instances disputing the
// same backlog.
type Worker struct {
	uow        ports.UnitOfWork
	outbox     ports.OutboxRepository
	publisher  ports.EventPublisher
	cfg        Config
	metrics    *metrics.Metrics
	logger     *zap.Logger
	instanceID string

	cancel context.CancelFunc
	done   chan struct{}
}

func New(uow ports.UnitOfWork, outbox ports.OutboxRepository, publisher ports.EventPublisher, cfg Config, m *metrics.Metrics, logger *zap.Logger) *Worker {
	return &Worker{
		uow:        uow,
		outbox:     outbox,
		publisher:  publisher,
		cfg:        cfg.withDefaults(),
		metrics:    m,
		logger:     logger,
		instanceID: instanceID(),
	}
}

func instanceID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
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
// first, Stop returns anyway: any event that cycle claimed but didn't
// finish publishing stays locked under this instance's id until
// LockDuration elapses, at which point any live instance's next poll
// reclaims and retries it — the same "always safely resumable" recovery
// path that handles a crashed instance.
func (w *Worker) Stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	select {
	case <-w.done:
	case <-ctx.Done():
		w.logger.Warn("outboxpublisher: shutdown grace period elapsed with a poll cycle still in flight")
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
	var claimed []ports.OutboxRecord
	err := w.uow.WithinTx(ctx, func(ctx context.Context) error {
		var err error
		claimed, err = w.outbox.ClaimBatch(ctx, w.cfg.BatchSize, w.instanceID, w.cfg.LockDuration)
		return err
	})
	if err != nil {
		w.logger.Error("outboxpublisher: claim batch failed", zap.Error(err))
		return
	}

	for _, rec := range claimed {
		w.publishOne(ctx, rec)
	}
}

// publishOne publishes a single already-claimed record and marks it
// published on success. A publish failure is left exactly as claimed —
// deliberately not retried immediately or requeued by hand — so it
// naturally becomes reclaimable once LockDuration elapses, by this
// instance's next poll or another's, per Config.LockDuration's doc
// comment.
func (w *Worker) publishOne(ctx context.Context, rec ports.OutboxRecord) {
	if rec.Attempts > 1 {
		// Attempts is incremented by ClaimBatch on every claim, including
		// the first — >1 means this row was reclaimed after a previous
		// attempt didn't finish (failed publish or a crashed instance).
		w.metrics.RecordRetry(ctx, metrics.RetryKindOutboxPublish)
	}
	if err := w.publisher.Publish(ctx, rec.EventID, rec.AggregateID, rec.EventType, rec.Payload); err != nil {
		w.logger.Error("outboxpublisher: publish failed, will retry after lock expires",
			zap.String("eventId", rec.EventID.String()),
			zap.String("eventType", rec.EventType),
			zap.Int("attempts", rec.Attempts),
			zap.Error(err),
		)
		return
	}

	if err := w.outbox.MarkPublished(ctx, rec.EventID, time.Now().UTC()); err != nil {
		// The event was actually delivered — this is exactly the
		// "interruption between publication and confirmation" scenario
		// the challenge calls out. It stays claimed until the lock
		// expires, then gets reclaimed and republished under the same
		// eventId; the destination is expected to tolerate that
		// duplicate by eventId (see SQSPublisher's doc comment).
		w.logger.Error("outboxpublisher: publish succeeded but marking it published failed",
			zap.String("eventId", rec.EventID.String()), zap.Error(err))
		return
	}

	w.logger.Info("outboxpublisher: published event",
		zap.String("eventId", rec.EventID.String()),
		zap.String("eventType", rec.EventType),
		zap.Int("attempts", rec.Attempts),
	)
}
