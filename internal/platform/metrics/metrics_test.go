package metrics_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

func scrape(t *testing.T, provider *metrics.Provider) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	provider.Handler.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code)
	return rec.Body.String()
}

func TestMetrics_RecordedValuesAppearOnScrape(t *testing.T) {
	provider, err := metrics.NewProvider()
	require.NoError(t, err)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	dlqDepth := func(ctx context.Context) (int64, error) { return 3, nil }
	outboxAge := func(ctx context.Context) (float64, bool, error) { return 42.5, true, nil }

	m, err := metrics.NewMetrics(provider.MeterProvider, dlqDepth, outboxAge)
	require.NoError(t, err)

	ctx := context.Background()
	m.RecordWagerTransaction(ctx, "PROCESSED", metrics.TransportHTTP, false)
	m.RecordWagerTransaction(ctx, "PROCESSED", metrics.TransportHTTP, true)
	m.RecordConcurrencyConflict(ctx, metrics.ConflictReasonIdempotencyConflict)
	m.RecordRetry(ctx, metrics.RetryKindPendingReference)
	m.RecordReconciliationDivergence(ctx)
	m.RecordProcessingDuration(ctx, metrics.TransportSQS, 20*time.Millisecond)

	body := scrape(t, provider)

	// Each check is a metric line that must contain every given substring
	// (name, label, value) — never assuming label order or adjacency,
	// since the OTel Prometheus exporter interleaves its own otel_scope_*
	// labels alphabetically among the ones this package defines.
	assertLineContainingAll(t, body, "wagering_transactions_total{", `status="PROCESSED"`, `transport="http"`, " 2")
	assertLineContainingAll(t, body, "idempotent_replays_total{", `transport="http"`, " 1")
	assertLineContainingAll(t, body, "concurrency_conflicts_total{", `reason="idempotency_conflict"`, " 1")
	assertLineContainingAll(t, body, "retries_total{", `kind="pending_reference"`, " 1")
	assertLineContainingAll(t, body, "reconciliation_divergences_total{", " 1")
	assertLineContainingAll(t, body, "dlq_messages{", " 3")
	assertLineContainingAll(t, body, "outbox_oldest_pending_age_seconds{", " 42.5")
	require.Contains(t, body, "wagering_processing_duration_seconds_bucket")
}

// assertLineContainingAll fails the test unless some line of body contains
// every one of want.
func assertLineContainingAll(t *testing.T, body string, want ...string) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		ok := true
		for _, w := range want {
			if !strings.Contains(line, w) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
	}
	t.Fatalf("expected a scrape line containing all of %q\n\ngot:\n%s", want, body)
}

func TestMetrics_OutboxDrained_GaugeAbsent(t *testing.T) {
	provider, err := metrics.NewProvider()
	require.NoError(t, err)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	outboxAge := func(ctx context.Context) (float64, bool, error) { return 0, false, nil }
	_, err = metrics.NewMetrics(provider.MeterProvider, nil, outboxAge)
	require.NoError(t, err)

	body := scrape(t, provider)
	require.False(t, strings.Contains(body, "outbox_oldest_pending_age_seconds "), "a fully drained outbox must not report a stale/zero age")
}
