package fxmodules

import (
	"context"

	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
)

// IDPModule provides the OIDC verifier used by every business route's auth
// middleware.
var IDPModule = fx.Module("idp",
	fx.Provide(newVerifier),
)

// newVerifier fetches the issuer's discovery document synchronously, as
// part of building the fx graph — an unreachable or misconfigured
// Keycloak fails the process at startup with a clear error, not on the
// first request that needs a token checked.
func newVerifier(cfg *config.Config) (*idp.Verifier, error) {
	return idp.NewVerifier(context.Background(), cfg.OIDC.IssuerURL, cfg.OIDC.Audience)
}
