package httpapi

import (
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
)

// statusRecorder captures the status code a handler wrote, so the logging
// middleware can report it — http.ResponseWriter itself does not expose it
// after the fact.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// recoveryMiddleware turns a panic in any handler into a 500 instead of
// crashing the process, and logs it with a stack trace for diagnosis.
// Business rejections must never panic — see processwagertransaction's own
// doc comment — so anything caught here is an actual bug or unexpected
// infrastructure failure.
func recoveryMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered",
						zap.Any("panic", rec),
						zap.String("correlationId", correlationIDFromContext(r.Context())),
						zap.Stack("stack"),
					)
					writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// correlationIDMiddleware honors an inbound X-Correlation-Id (a provider
// tracing its own request across systems) or generates one, threads it
// through the context for logs and outbox events, and echoes it back so
// the caller can correlate the response with their own logs.
func correlationIDMiddleware(idGen func() string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Correlation-Id")
			if id == "" {
				id = idGen()
			}
			w.Header().Set("X-Correlation-Id", id)
			next.ServeHTTP(w, r.WithContext(withCorrelationID(r.Context(), id)))
		})
	}
}

// loggingMiddleware logs one structured line per request. Per the
// observability checklist, it never logs the Authorization header, the
// bearer token, or the request/response body — only identifiers.
func loggingMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			logger.Info("http_request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", rec.status),
				zap.Duration("duration", time.Since(start)),
				zap.String("correlationId", correlationIDFromContext(r.Context())),
			)
		})
	}
}

// authMiddleware requires a valid "Authorization: Bearer <token>" header,
// verified against the configured IdP, and injects the resulting claims
// into the request context for downstream authorization checks. Every
// business route must sit behind this — there is no exception.
func authMiddleware(verifier *idp.Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			token, ok := strings.CutPrefix(header, "Bearer ")
			if !ok || token == "" {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token")
				return
			}

			claims, err := verifier.Verify(r.Context(), token)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or expired token")
				return
			}

			next.ServeHTTP(w, r.WithContext(withClaims(r.Context(), claims)))
		})
	}
}

// requireInternal restricts a route to the internal-service client
// (wallet management, reconciliation) — no external provider client
// carries this claim, so a provider token is rejected here regardless of
// how it was obtained.
func requireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !claimsFromContext(r.Context()).Internal {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "this operation is restricted to the internal service")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireProvider restricts a route to any authenticated external provider
// (a non-empty providerId claim) — used for routes where the operation's
// own providerId is validated separately (from the body, or against a
// loaded resource), not from the URL.
func requireProvider(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if claimsFromContext(r.Context()).ProviderID == "" {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "this operation requires a provider identity")
			return
		}
		next.ServeHTTP(w, r)
	})
}
