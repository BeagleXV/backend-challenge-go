package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/fxmodules"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

func main() {
	// Loaded outside fx so a missing/invalid environment variable fails
	// fast with a plain message, before dig's dependency graph exists.
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fx.New(fxmodules.All(cfg)...).Run()
}
