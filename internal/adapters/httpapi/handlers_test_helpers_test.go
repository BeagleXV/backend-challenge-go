package httpapi

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func zapNop() *zap.Logger { return zap.NewNop() }

// withChiParam attaches a chi URL param to r's context, so a handler can be
// invoked directly (bypassing the router) while still resolving
// chi.URLParam(r, name) as if chi had matched the route.
func withChiParam(r *http.Request, name, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(name, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}
