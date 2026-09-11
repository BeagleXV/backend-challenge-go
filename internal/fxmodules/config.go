package fxmodules

import (
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// ConfigModule supplies the already-loaded Config. Loading happens in
// cmd/api/main.go, before fx.New is even called, so a bad/missing
// environment variable fails the process immediately with a plain error
// message on stderr — not several layers deep inside dig's dependency
// graph.
func ConfigModule(cfg *config.Config) fx.Option {
	return fx.Module("config", fx.Supply(cfg))
}
