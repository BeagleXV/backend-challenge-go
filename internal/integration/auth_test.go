//go:build integration

package integration_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestAuth_RealKeycloak covers the challenge's required auth scenarios
// against a real IdP — never the fake JWKS server
// internal/adapters/idp/verifier_test.go uses for its own unit tests: a
// missing token, a malformed/invalid one and an expired one must all be
// rejected with 401, and one provider's token must never reach another
// provider's resources.
func TestAuth_RealKeycloak(t *testing.T) {
	pg := startPostgres(t)
	sqsInf := startLocalStack(t, 5)
	kc := startKeycloak(t)
	app := startApp(t, pg, sqsInf, kc)

	internalToken := kc.token(t, "internal-service", "internal-service-secret-local-only")
	playerID := uuid.New()
	wallet := openWalletViaHTTP(t, app, internalToken, playerID, "100.00")

	t.Run("missing token is rejected", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, app.baseURL+"/wallets/"+wallet.ID.String(), nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("malformed token is rejected", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, app.baseURL+"/wallets/"+wallet.ID.String(), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer not-a-real-jwt")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("expired token is rejected", func(t *testing.T) {
		// provider-a-shortlived is configured (deploy/keycloak/realm-export.json)
		// with access.token.lifespan=2s specifically so this test can wait
		// out a real expiry instead of needing a forged/tampered token.
		shortLived := kc.token(t, "provider-a-shortlived", "provider-a-shortlived-secret-local-only")
		time.Sleep(3 * time.Second)

		req, err := http.NewRequest(http.MethodGet, app.baseURL+"/wallets/"+wallet.ID.String(), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+shortLived)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		// provider-a-shortlived is not internal anyway (403 would also be a
		// legitimate rejection for that reason alone), but an expired token
		// must never even get that far — 401 either way proves the verifier
		// checked expiry before any authorization decision ran.
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("provider isolation: provider B cannot see provider A's wager transaction", func(t *testing.T) {
		providerAToken := kc.token(t, "provider-a", "provider-a-secret-local-only")
		providerBToken := kc.token(t, "provider-b", "provider-b-secret-local-only")

		submitted := submitWagerViaHTTP(t, app, providerAToken, wallet.ID, playerID, "bet-auth-isolation-1")

		req, err := http.NewRequest(http.MethodGet, app.baseURL+"/wagering/transactions/"+submitted.TransactionID.String(), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+providerBToken)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode, "a transaction must be invisible to a provider that did not create it")
	})
}
