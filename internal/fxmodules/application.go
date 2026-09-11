package fxmodules

import (
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
	"github.com/beaglexv/backend-challenge-go/internal/platform/clock"
	"github.com/beaglexv/backend-challenge-go/internal/platform/idgen"
)

// ApplicationModule provides ports.Clock/ports.IDGenerator's real
// implementations and every use-case service. Services are stateless
// beyond their dependencies, so fx's default singleton scope (one instance
// shared by the HTTP handlers and SQS consumer added in later phases) is
// exactly what processwagertransaction.Service's doc comment expects.
var ApplicationModule = fx.Module("application",
	fx.Provide(
		asClock,
		asIDGenerator,
		openwallet.New,
		processwagertransaction.New,
		reconciliation.New,
		resolvependingreference.New,
	),
)

func asClock() ports.Clock { return clock.New() }

func asIDGenerator() ports.IDGenerator { return idgen.New() }
