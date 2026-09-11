package httpapi

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

func mustMoney(t *testing.T, amount string, currency money.Currency) money.Money {
	t.Helper()
	m, err := money.New(amount, currency)
	require.NoError(t, err)
	return m
}

func TestPayloadHash_DeterministicForEquivalentRequests(t *testing.T) {
	req := wagerTransactionRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "tx-1",
		PlayerID:              uuid.New(),
		WalletID:              uuid.New(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 mustMoney(t, "25.00", money.BRL),
	}

	h1, err := payloadHash(req)
	require.NoError(t, err)
	h2, err := payloadHash(req)
	require.NoError(t, err)

	assert.Equal(t, h1, h2)
}

func TestPayloadHash_DiffersOnBusinessFieldChange(t *testing.T) {
	base := wagerTransactionRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "tx-1",
		PlayerID:              uuid.New(),
		WalletID:              uuid.New(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 mustMoney(t, "25.00", money.BRL),
	}
	changed := base
	changed.Money = mustMoney(t, "30.00", money.BRL)

	h1, err := payloadHash(base)
	require.NoError(t, err)
	h2, err := payloadHash(changed)
	require.NoError(t, err)

	assert.NotEqual(t, h1, h2)
}
