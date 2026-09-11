package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Transport identifies which entry point processed a wager operation.
// Never a label with unbounded cardinality (playerId, transactionId,
// externalTransactionId) — every label used in this package is a small,
// fixed set of known values, per the challenge's own instruction not to
// leak high-cardinality business data into metric labels.
type Transport string

const (
	TransportHTTP   Transport = "http"
	TransportSQS    Transport = "sqs"
	TransportWorker Transport = "worker"
)

// ConflictReason identifies the kind of concurrency conflict observed —
// two callers racing to do the same thing, one of them losing safely
// rather than corrupting anything.
type ConflictReason string

const (
	ConflictReasonIdempotencyConflict  ConflictReason = "idempotency_conflict"
	ConflictReasonExternalIDReused     ConflictReason = "external_id_reused"
	ConflictReasonPendingReferenceRace ConflictReason = "pending_reference_race"
)

// RetryKind identifies which of this system's three independent retry
// mechanisms fired.
type RetryKind string

const (
	RetryKindSQSMessage       RetryKind = "sqs_message"
	RetryKindPendingReference RetryKind = "pending_reference"
	RetryKindOutboxPublish    RetryKind = "outbox_publish"
)

// DLQDepthFunc reports the current approximate message count on the
// wager-transactions DLQ. Called only when something scrapes /metrics
// (see Provider's doc comment) — never on a timer.
type DLQDepthFunc func(ctx context.Context) (int64, error)

// OutboxOldestPendingAgeFunc reports how long, in seconds, the oldest
// unpublished outbox event has been waiting, and whether one exists at
// all (false when the outbox is fully drained — a healthy, not a missing,
// state).
type OutboxOldestPendingAgeFunc func(ctx context.Context) (age float64, exists bool, err error)

// Metrics holds every instrument this codebase records to. See the
// package doc comment for why they're centralized here instead of
// declared ad hoc at each call site.
type Metrics struct {
	wagerTransactionsTotal         metric.Int64Counter
	idempotentReplaysTotal         metric.Int64Counter
	concurrencyConflictsTotal      metric.Int64Counter
	retriesTotal                   metric.Int64Counter
	reconciliationDivergencesTotal metric.Int64Counter
	processingDuration             metric.Float64Histogram
}

// NewNoop returns a Metrics backed by the OTel no-op meter provider —
// every Record*/instrument call becomes a cheap no-op. For tests of code
// that depends on *Metrics but isn't itself testing what gets recorded;
// production always wires the real provider via fx (internal/fxmodules).
func NewNoop() *Metrics {
	m, err := NewMetrics(noop.NewMeterProvider(), nil, nil)
	if err != nil {
		// Unreachable: the no-op meter provider never rejects instrument
		// creation, and the two callbacks are both nil here.
		panic(fmt.Sprintf("metrics: NewNoop: %v", err))
	}
	return m
}

// NewMetrics creates every instrument, registering the two observable
// gauges (DLQ depth, outbox lag) with the callbacks supplied. Either
// callback can be nil in a test/degraded setup — the gauge then simply
// never reports a value, rather than the whole provider failing to build.
func NewMetrics(mp metric.MeterProvider, dlqDepth DLQDepthFunc, outboxOldestPendingAge OutboxOldestPendingAgeFunc) (*Metrics, error) {
	meter := mp.Meter("backend-challenge-go")

	wagerTransactionsTotal, err := meter.Int64Counter(
		"wagering_transactions_total",
		metric.WithDescription("Wager operations processed, by outcome status and entry point."),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create wagering_transactions_total: %w", err)
	}

	idempotentReplaysTotal, err := meter.Int64Counter(
		"idempotent_replays_total",
		metric.WithDescription("Requests answered from a previously persisted result instead of being reprocessed."),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create idempotent_replays_total: %w", err)
	}

	concurrencyConflictsTotal, err := meter.Int64Counter(
		"concurrency_conflicts_total",
		metric.WithDescription("Times two callers raced to do the same thing and one lost safely, by reason."),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create concurrency_conflicts_total: %w", err)
	}

	retriesTotal, err := meter.Int64Counter(
		"retries_total",
		metric.WithDescription("Retry attempts across this system's independent retry mechanisms, by kind."),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create retries_total: %w", err)
	}

	reconciliationDivergencesTotal, err := meter.Int64Counter(
		"reconciliation_divergences_total",
		metric.WithDescription("Reconciliation runs that found the stored balance did not match the ledger."),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create reconciliation_divergences_total: %w", err)
	}

	processingDuration, err := meter.Float64Histogram(
		"wagering_processing_duration_seconds",
		metric.WithDescription("Time to process one wager operation end to end, by entry point."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: create wagering_processing_duration_seconds: %w", err)
	}

	if dlqDepth != nil {
		gauge, err := meter.Int64ObservableGauge(
			"dlq_messages",
			metric.WithDescription("Approximate number of messages currently on the wager-transactions DLQ."),
		)
		if err != nil {
			return nil, fmt.Errorf("metrics: create dlq_messages: %w", err)
		}
		if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			depth, err := dlqDepth(ctx)
			if err != nil {
				return err
			}
			o.ObserveInt64(gauge, depth)
			return nil
		}, gauge); err != nil {
			return nil, fmt.Errorf("metrics: register dlq_messages callback: %w", err)
		}
	}

	if outboxOldestPendingAge != nil {
		gauge, err := meter.Float64ObservableGauge(
			"outbox_oldest_pending_age_seconds",
			metric.WithDescription("Age of the oldest unpublished outbox event; absent when the outbox is fully drained."),
			metric.WithUnit("s"),
		)
		if err != nil {
			return nil, fmt.Errorf("metrics: create outbox_oldest_pending_age_seconds: %w", err)
		}
		if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			age, exists, err := outboxOldestPendingAge(ctx)
			if err != nil {
				return err
			}
			if exists {
				o.ObserveFloat64(gauge, age)
			}
			return nil
		}, gauge); err != nil {
			return nil, fmt.Errorf("metrics: register outbox_oldest_pending_age_seconds callback: %w", err)
		}
	}

	return &Metrics{
		wagerTransactionsTotal:         wagerTransactionsTotal,
		idempotentReplaysTotal:         idempotentReplaysTotal,
		concurrencyConflictsTotal:      concurrencyConflictsTotal,
		retriesTotal:                   retriesTotal,
		reconciliationDivergencesTotal: reconciliationDivergencesTotal,
		processingDuration:             processingDuration,
	}, nil
}

// RecordWagerTransaction counts one processed wager operation by its
// terminal (or interim, for PENDING_REFERENCE) status, and separately
// counts it as an idempotent replay when it was one — both dimensions
// matter independently (a replay is still, e.g., PROCESSED).
func (m *Metrics) RecordWagerTransaction(ctx context.Context, status string, transport Transport, idempotentReplay bool) {
	attrs := metric.WithAttributes(statusAttr(status), transportAttr(transport))
	m.wagerTransactionsTotal.Add(ctx, 1, attrs)
	if idempotentReplay {
		m.idempotentReplaysTotal.Add(ctx, 1, metric.WithAttributes(transportAttr(transport)))
	}
}

// RecordProcessingDuration records how long one Handle/Resume/Expire call
// took, regardless of its outcome (success, business rejection, or error)
// — latency is a property of the call, not of whether it succeeded.
func (m *Metrics) RecordProcessingDuration(ctx context.Context, transport Transport, d time.Duration) {
	m.processingDuration.Record(ctx, d.Seconds(), metric.WithAttributes(transportAttr(transport)))
}

func (m *Metrics) RecordConcurrencyConflict(ctx context.Context, reason ConflictReason) {
	m.concurrencyConflictsTotal.Add(ctx, 1, metric.WithAttributes(reasonAttr(reason)))
}

func (m *Metrics) RecordRetry(ctx context.Context, kind RetryKind) {
	m.retriesTotal.Add(ctx, 1, metric.WithAttributes(kindAttr(kind)))
}

func (m *Metrics) RecordReconciliationDivergence(ctx context.Context) {
	m.reconciliationDivergencesTotal.Add(ctx, 1)
}
