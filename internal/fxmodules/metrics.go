package fxmodules

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// MetricsModule provides the OTel/Prometheus provider and every instrument
// (metrics.Metrics), and starts a dedicated HTTP server on cfg.MetricsAddr
// serving only /metrics — separate from the business API's own listener,
// so it can be bound to a private interface or firewalled off without
// touching the public port, and so a slow/stuck business handler can never
// block a scrape (they don't share a listener or goroutine pool).
var MetricsModule = fx.Module("metrics",
	fx.Provide(newMetricsProvider, newMetrics),
	fx.Invoke(registerMetricsServer),
)

func newMetricsProvider() (*metrics.Provider, error) {
	return metrics.NewProvider()
}

func newMetrics(provider *metrics.Provider, client *sqs.Client, cfg *config.Config, outbox ports.OutboxRepository) (*metrics.Metrics, error) {
	dlqDepth := func(ctx context.Context) (int64, error) {
		out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(cfg.SQS.WagerTransactionsDLQURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		if err != nil {
			return 0, err
		}
		raw := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
		var depth int64
		if _, err := fmt.Sscanf(raw, "%d", &depth); err != nil {
			return 0, fmt.Errorf("parse ApproximateNumberOfMessages %q: %w", raw, err)
		}
		return depth, nil
	}

	outboxOldestPendingAge := func(ctx context.Context) (float64, bool, error) {
		occurredAt, exists, err := outbox.OldestUnpublishedOccurredAt(ctx)
		if err != nil || !exists {
			return 0, false, err
		}
		return time.Since(occurredAt).Seconds(), true, nil
	}

	return metrics.NewMetrics(provider.MeterProvider, dlqDepth, outboxOldestPendingAge)
}

func registerMetricsServer(lc fx.Lifecycle, cfg *config.Config, provider *metrics.Provider, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", provider.Handler)
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("metrics server exited unexpectedly", zap.Error(err))
				}
			}()
			logger.Info("metrics server listening", zap.String("addr", cfg.MetricsAddr))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := srv.Shutdown(ctx); err != nil {
				return err
			}
			return provider.Shutdown(ctx)
		},
	})
}
