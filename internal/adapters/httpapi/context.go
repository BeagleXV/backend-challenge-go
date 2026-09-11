package httpapi

import (
	"context"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
)

type contextKey int

const (
	claimsContextKey contextKey = iota
	correlationIDContextKey
)

func withClaims(ctx context.Context, claims idp.Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// claimsFromContext returns the authenticated caller's claims. Only called
// from handlers reached after authMiddleware, which always sets it — a
// missing value is a wiring bug, not a request-time condition, so it is
// not reported as an error.
func claimsFromContext(ctx context.Context) idp.Claims {
	claims, _ := ctx.Value(claimsContextKey).(idp.Claims)
	return claims
}

func withCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDContextKey, id)
}

func correlationIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDContextKey).(string)
	return id
}
