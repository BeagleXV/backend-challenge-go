package idp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	josejwt "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
)

// fakeIdP is a minimal OIDC discovery + JWKS server backed by a real RSA
// key, so idp.Verifier is exercised against real signature/issuer/audience
// checks rather than a mock of the verifier itself. Real Keycloak
// integration (a running IdP, not this stand-in) is covered by Fase 15.
type fakeIdP struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	keyID  string
}

func newFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	f := &fakeIdP{key: key, keyID: "test-key-1"}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                 f.server.URL,
			"jwks_uri":               f.server.URL + "/jwks",
			"authorization_endpoint": f.server.URL + "/auth",
			"token_endpoint":         f.server.URL + "/token",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		jwk := josejwt.JSONWebKey{Key: &f.key.PublicKey, KeyID: f.keyID, Algorithm: "RS256", Use: "sig"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(josejwt.JSONWebKeySet{Keys: []josejwt.JSONWebKey{jwk}})
	})
	f.server = httptest.NewServer(mux)
	return f
}

func (f *fakeIdP) issueToken(t *testing.T, claims map[string]any, expiry time.Duration) string {
	t.Helper()
	signer, err := josejwt.NewSigner(josejwt.SigningKey{Algorithm: josejwt.RS256, Key: f.key}, &josejwt.SignerOptions{
		ExtraHeaders: map[josejwt.HeaderKey]any{"kid": f.keyID},
	})
	require.NoError(t, err)

	base := map[string]any{
		"iss": f.server.URL,
		"exp": time.Now().Add(expiry).Unix(),
		"iat": time.Now().Unix(),
		"sub": "test-subject",
	}
	for k, v := range claims {
		base[k] = v
	}
	b, err := json.Marshal(base)
	require.NoError(t, err)

	sig, err := signer.Sign(b)
	require.NoError(t, err)
	compact, err := sig.CompactSerialize()
	require.NoError(t, err)
	return compact
}

func (f *fakeIdP) close() { f.server.Close() }

func TestVerifier_AcceptsValidToken(t *testing.T) {
	fake := newFakeIdP(t)
	defer fake.close()

	verifier, err := idp.NewVerifier(context.Background(), fake.server.URL, "wagering-api")
	require.NoError(t, err)

	token := fake.issueToken(t, map[string]any{
		"aud":        "wagering-api",
		"azp":        "provider-a",
		"providerId": "provider-a",
	}, time.Hour)

	claims, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, "provider-a", claims.ProviderID)
	assert.Equal(t, "provider-a", claims.ClientID)
	assert.False(t, claims.Internal)
}

func TestVerifier_RejectsExpiredToken(t *testing.T) {
	fake := newFakeIdP(t)
	defer fake.close()

	verifier, err := idp.NewVerifier(context.Background(), fake.server.URL, "wagering-api")
	require.NoError(t, err)

	token := fake.issueToken(t, map[string]any{"aud": "wagering-api"}, -time.Hour)

	_, err = verifier.Verify(context.Background(), token)
	assert.Error(t, err)
}

func TestVerifier_RejectsWrongAudience(t *testing.T) {
	fake := newFakeIdP(t)
	defer fake.close()

	verifier, err := idp.NewVerifier(context.Background(), fake.server.URL, "wagering-api")
	require.NoError(t, err)

	token := fake.issueToken(t, map[string]any{"aud": "some-other-api"}, time.Hour)

	_, err = verifier.Verify(context.Background(), token)
	assert.Error(t, err)
}

func TestVerifier_RejectsEmptyToken(t *testing.T) {
	fake := newFakeIdP(t)
	defer fake.close()

	verifier, err := idp.NewVerifier(context.Background(), fake.server.URL, "wagering-api")
	require.NoError(t, err)

	_, err = verifier.Verify(context.Background(), "")
	assert.Error(t, err)
}

func TestVerifier_RejectsMalformedToken(t *testing.T) {
	fake := newFakeIdP(t)
	defer fake.close()

	verifier, err := idp.NewVerifier(context.Background(), fake.server.URL, "wagering-api")
	require.NoError(t, err)

	_, err = verifier.Verify(context.Background(), "not-a-jwt")
	assert.Error(t, err)
}

func TestNewVerifier_UnreachableIssuer_FailsFast(t *testing.T) {
	_, err := idp.NewVerifier(context.Background(), "http://127.0.0.1:1/nonexistent", "wagering-api")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "idp:")
}
