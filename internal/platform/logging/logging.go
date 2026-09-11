// Package logging builds the process-wide zap logger from Config, so every
// component logs through the same structured, leveled sink.
package logging

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// New builds a zap.Logger appropriate for cfg.AppEnv: human-readable console
// output for "local"/"dev", JSON for everything else (what a log shipper in
// a real environment expects).
func New(cfg *config.Config) (*zap.Logger, error) {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return nil, fmt.Errorf("logging: invalid LOG_LEVEL %q: %w", cfg.LogLevel, err)
	}

	var zapCfg zap.Config
	switch cfg.AppEnv {
	case "local", "dev":
		zapCfg = zap.NewDevelopmentConfig()
	default:
		zapCfg = zap.NewProductionConfig()
	}
	zapCfg.Level = zap.NewAtomicLevelAt(level)

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("logging: build logger: %w", err)
	}
	return logger, nil
}
