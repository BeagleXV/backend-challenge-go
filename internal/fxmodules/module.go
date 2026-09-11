// Package fxmodules wires the application via Uber Fx. Each file is one
// fx.Module per layer (config, logging, postgres, application, ...); All
// aggregates them for cmd/api/main.go.
package fxmodules

import (
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// All returns every fx.Option needed to build and run the process, given
// the already-loaded, already-validated Config.
func All(cfg *config.Config) []fx.Option {
	return []fx.Option{
		ConfigModule(cfg),
		LoggingModule,
		PostgresModule,
		ApplicationModule,
		IDPModule,
		// Every worker module is listed before HTTPAPIModule so its
		// OnStart hook is appended first: fx runs OnStop in reverse
		// order, so on shutdown the HTTP listener stops accepting new
		// requests before any worker stops pulling new work, matching
		// the README's documented order (HTTP listener → workers →
		// connections) rather than an arbitrary one.
		SQSConsumerModule,
		ReferenceWorkerModule,
		OutboxPublisherModule,
		HTTPAPIModule,
		fx.StopTimeout(cfg.ShutdownTimeout),
	}
}
