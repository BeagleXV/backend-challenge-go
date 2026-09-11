// Package metrics wires OpenTelemetry Metrics with a Prometheus exporter
// and defines every instrument this codebase records to. Centralizing the
// instrument definitions here — rather than letting each package call
// meter.Int64Counter("some-name-it-invented") inline — means every metric
// name and label set is declared exactly once, so two call sites can never
// silently drift into using the same name with different label keys (which
// the Prometheus client would reject at scrape time) or two different
// names for what should be the same series.
package metrics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Provider owns the OTel MeterProvider and the Prometheus registry/handler
// that expose everything recorded through it on /metrics. There is no
// push exporter and no background collection loop: the Prometheus
// exporter is pull-based, and every observable (asynchronous) instrument's
// callback only runs when promhttp actually serves a scrape — so a gauge
// like "outbox oldest pending age" is always as fresh as the last scrape,
// never a stale value from a timer that drifted.
type Provider struct {
	MeterProvider *sdkmetric.MeterProvider
	Handler       http.Handler
}

// NewProvider builds the provider. Nothing about it depends on any adapter in this
// codebase — that's deliberate: instruments.go's Metrics struct is what
// adapters actually depend on, keeping "how metrics are exported" and
// "what we measure" as two separable concerns, the second of which is
// reusable even if the exporter changes later (e.g. to add tracing, which
// the README lists as an optional differentiator this setup does not
// preclude).
func NewProvider() (*Provider, error) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("metrics: create prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	return &Provider{
		MeterProvider: mp,
		Handler:       promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}, nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return p.MeterProvider.Shutdown(ctx)
}
