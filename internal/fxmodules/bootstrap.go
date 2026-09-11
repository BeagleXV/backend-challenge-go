package fxmodules

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
)

// BootstrapModule exists to force fx to build the whole dependency graph —
// config, logging, the Postgres pool and every use case — even though
// nothing in this phase yet consumes them as an HTTP server or worker
// would. It also registers the top-level startup/shutdown log lines. Phase
// 6 (HTTP) and Phases 8-11 (SQS/workers) will depend on the same services
// directly, at which point this fx.Invoke becomes redundant and should be
// removed rather than kept alongside it.
var BootstrapModule = fx.Module("bootstrap",
	fx.Invoke(registerBootstrap),
)

func registerBootstrap(
	lc fx.Lifecycle,
	logger *zap.Logger,
	_ *openwallet.Service,
	_ *processwagertransaction.Service,
	_ *reconciliation.Service,
	_ *resolvependingreference.Service,
) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("wagering-api started")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("wagering-api stopping")
			return nil
		},
	})
}
