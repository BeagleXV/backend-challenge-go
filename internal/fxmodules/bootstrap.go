package fxmodules

import (
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/application/resolvependingreference"
)

// BootstrapModule forces fx to build the parts of the graph nothing else
// depends on yet. Since Fase 6, HTTPAPIModule's own fx.Invoke already
// forces config/logging/postgres/idp and the use cases it calls directly
// (openwallet, processwagertransaction, reconciliation) — this module only
// needs to reach resolvependingreference.Service, which stays unused until
// the pending-reference worker (Fase 10) depends on it directly, at which
// point this fx.Invoke becomes redundant and should be removed.
var BootstrapModule = fx.Module("bootstrap",
	fx.Invoke(func(*resolvependingreference.Service) {}),
)
