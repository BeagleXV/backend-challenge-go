package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
)

func TestRecoveryMiddleware_TurnsPanicInto500(t *testing.T) {
	panics := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	handler := recoveryMiddleware(zapNop())(panics)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	assert.NotPanics(t, func() { handler.ServeHTTP(rec, r) })
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestCorrelationIDMiddleware_GeneratesWhenAbsent(t *testing.T) {
	var seenInContext string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenInContext = correlationIDFromContext(r.Context())
	})
	handler := correlationIDMiddleware(func() string { return "generated-id" })(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, "generated-id", seenInContext)
	assert.Equal(t, "generated-id", rec.Header().Get("X-Correlation-Id"))
}

func TestCorrelationIDMiddleware_HonorsInbound(t *testing.T) {
	var seenInContext string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenInContext = correlationIDFromContext(r.Context())
	})
	handler := correlationIDMiddleware(func() string { return "should-not-be-used" })(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Correlation-Id", "inbound-id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, "inbound-id", seenInContext)
}

func TestRequireInternal_RejectsProviderClaims(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	handler := requireInternal(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(withClaims(r.Context(), idp.Claims{ProviderID: "provider-a"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.False(t, called)
}

func TestRequireInternal_AllowsInternalClaims(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	handler := requireInternal(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(withClaims(r.Context(), idp.Claims{Internal: true}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.True(t, called)
}

func TestRequireProvider_RejectsInternalClaims(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	handler := requireProvider(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(withClaims(r.Context(), idp.Claims{Internal: true}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.False(t, called)
}

func TestAuthMiddleware_MissingHeader_Returns401(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next must not be called without a valid token")
	})
	handler := authMiddleware(nil)(next)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
