package fxmodules

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/referenceworker"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

// ReferenceWorkerModule registers the pending-reference worker's
// lifecycle: OnStart launches the poll loop, OnStop stops it (bounded by
// fx.StopTimeout, same as every other component's graceful shutdown).
// This is also, since Fase 10, what makes BootstrapModule's old
// placeholder invoke of resolvependingreference.Service redundant —
// removed along with it.
var ReferenceWorkerModule = fx.Module("referenceworker",
	fx.Invoke(registerReferenceWorker),
)

func registerReferenceWorker(lc fx.Lifecycle, resolver *resolvependingreference.Service, m *metrics.Metrics, logger *zap.Logger) {
	worker := referenceworker.New(resolver, referenceworker.Config{}, m, logger)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			worker.Start()
			logger.Info("pending-reference worker started")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return worker.Stop(ctx)
		},
	})
}
