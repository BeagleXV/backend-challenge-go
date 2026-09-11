// Package idp validates bearer tokens issued by an external OAuth
// 2.0/OIDC identity provider (Keycloak). It never issues or stores
// credentials itself — that is out of scope per the challenge — it only
// verifies signature, issuer and audience, and extracts the claims the
// authorization middleware (internal/adapters/httpapi) needs.
package idp

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Claims is the subset of an access token's claims the application cares
// about. providerId is a hardcoded claim configured per external-provider
// client in Keycloak (client_credentials has no end user, so there is no
// standard claim to repurpose); internal marks the service account used
// for wallet-management/reconciliation operations, which no external
// provider client carries.
type Claims struct {
	Subject    string `json:"sub"`
	ClientID   string `json:"azp"`
	ProviderID string `json:"providerId"`
	Internal   bool   `json:"internal"`
}

// Verifier validates raw JWT bearer tokens against one OIDC issuer.
type Verifier struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// NewVerifier fetches the issuer's discovery document and JWKS endpoint
// synchronously — an unreachable or misconfigured IdP fails process
// startup immediately (fx builds this in the "idp" module's Provide,
// before OnStart), rather than surfacing as a mysterious 401 on the first
// real request.
func NewVerifier(ctx context.Context, issuerURL, audience string) (*Verifier, error) {
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("idp: discover issuer %q: %w", issuerURL, err)
	}
	verifier := provider.VerifierContext(ctx, &oidc.Config{ClientID: audience})
	return &Verifier{provider: provider, verifier: verifier}, nil
}

// ErrInvalidToken wraps every reason a bearer token is rejected: missing,
// malformed, expired, wrong issuer/audience, or bad signature. The caller
// (auth middleware) does not need to distinguish further — all of them
// mean 401.
type ErrInvalidToken struct{ cause error }

func (e *ErrInvalidToken) Error() string { return fmt.Sprintf("idp: invalid token: %v", e.cause) }
func (e *ErrInvalidToken) Unwrap() error { return e.cause }

// Verify validates rawToken (without any "Bearer " prefix) and returns its
// claims.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Claims, error) {
	rawToken = strings.TrimSpace(rawToken)
	if rawToken == "" {
		return Claims{}, &ErrInvalidToken{cause: fmt.Errorf("empty token")}
	}

	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Claims{}, &ErrInvalidToken{cause: err}
	}

	var claims Claims
	if err := idToken.Claims(&claims); err != nil {
		return Claims{}, &ErrInvalidToken{cause: fmt.Errorf("decode claims: %w", err)}
	}
	return claims, nil
}
