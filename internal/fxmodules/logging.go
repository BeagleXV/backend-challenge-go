package fxmodules

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/platform/logging"
)

// LoggingModule provides the process-wide *zap.Logger and flushes it on
// shutdown. Sync errors are ignored: on Linux, syncing stdout/stderr
// reliably returns ENOTTY/EINVAL when the process is attached to a
// terminal or a pipe, which is not a real failure.
var LoggingModule = fx.Module("logging",
	fx.Provide(logging.New),
	fx.Invoke(registerLoggerSync),
)

func registerLoggerSync(lc fx.Lifecycle, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			_ = logger.Sync()
			return nil
		},
	})
}
