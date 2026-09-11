package fxmodules

import (
	"context"
	"errors"
	"net"
	"net/http"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/httpapi"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// HTTPAPIModule provides the chi router and starts/stops the HTTP server
// through fx.Lifecycle.
var HTTPAPIModule = fx.Module("httpapi",
	fx.Provide(httpapi.NewRouter),
	fx.Invoke(registerHTTPServer),
)

func registerHTTPServer(lc fx.Lifecycle, cfg *config.Config, handler http.Handler, logger *zap.Logger) {
	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: handler,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("http server exited unexpectedly", zap.Error(err))
				}
			}()
			logger.Info("http server listening", zap.String("addr", cfg.HTTPAddr))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Stops accepting new connections and waits (bounded by ctx,
			// which fx derives from cfg.ShutdownTimeout via
			// fx.StopTimeout) for in-flight requests to finish before
			// returning — never drops a request mid-processing.
			return srv.Shutdown(ctx)
		},
	})
}
