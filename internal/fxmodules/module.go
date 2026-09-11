// Package fxmodules wires the application via Uber Fx. Each file is one
// fx.Module per layer (config, logging, postgres, application, ...); All
// aggregates them for cmd/api/main.go. HTTP, SQS and worker modules are
// added by the phases that implement those adapters (6, 8-11) — until
// then, BootstrapModule is what forces the graph to build.
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
		HTTPAPIModule,
		BootstrapModule,
		fx.StopTimeout(cfg.ShutdownTimeout),
	}
}
