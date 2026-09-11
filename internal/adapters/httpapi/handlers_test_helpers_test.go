package httpapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func zapNop() *zap.Logger { return zap.NewNop() }

func mustMoney(t *testing.T, amount string, currency money.Currency) money.Money {
	t.Helper()
	m, err := money.New(amount, currency)
	require.NoError(t, err)
	return m
}

// withChiParam attaches a chi URL param to r's context, so a handler can be
// invoked directly (bypassing the router) while still resolving
// chi.URLParam(r, name) as if chi had matched the route.
func withChiParam(r *http.Request, name, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(name, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}
