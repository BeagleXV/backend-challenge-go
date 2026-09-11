package processwagertransaction_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

func baseCanonicalRequest(t *testing.T) processwagertransaction.Request {
	t.Helper()
	return processwagertransaction.Request{
		IdempotencyKey:        "provider-a:tx-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "tx-1",
		WalletID:              uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		PlayerID:              uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  wagertransaction.KindBet,
		Amount:                mustMoney(t, "25.00"),
	}
}

func TestCanonicalHash_DeterministicForIdenticalInput(t *testing.T) {
	req := baseCanonicalRequest(t)
	assert.Equal(t, processwagertransaction.CanonicalHash(req), processwagertransaction.CanonicalHash(req))
}

func TestCanonicalHash_IgnoresTransportMetadata(t *testing.T) {
	req1 := baseCanonicalRequest(t)
	req2 := req1
	req2.IdempotencyKey = "a-totally-different-key"
	req2.CorrelationID = "some-correlation-id"
	req2.Inbox = &processwagertransaction.InboxInfo{ConsumerName: "sqs", MessageID: "msg-1", Hash: "irrelevant"}

	assert.Equal(t, processwagertransaction.CanonicalHash(req1), processwagertransaction.CanonicalHash(req2),
		"idempotencyKey, correlationId and inbox metadata must never affect the business hash")
}

func TestCanonicalHash_NormalizesEquivalentAmountForms(t *testing.T) {
	req1 := baseCanonicalRequest(t)
	req2 := req1
	amount, err := money.New("25", money.BRL) // bare integer part, no decimals
	require.NoError(t, err)
	req2.Amount = amount

	assert.Equal(t, processwagertransaction.CanonicalHash(req1), processwagertransaction.CanonicalHash(req2),
		"\"25\" and \"25.00\" are the same amount and must hash identically")
}

func TestCanonicalHash_DiffersOnEachBusinessField(t *testing.T) {
	base := baseCanonicalRequest(t)
	baseHash := processwagertransaction.CanonicalHash(base)

	mutations := map[string]processwagertransaction.Request{
		"providerId": withProviderID(base, "provider-b"),
		"externalTransactionId": func() processwagertransaction.Request {
			r := base
			r.ExternalTransactionID = "tx-2"
			return r
		}(),
		"playerId": func() processwagertransaction.Request {
			r := base
			r.PlayerID = uuid.MustParse("00000000-0000-0000-0000-000000000099")
			return r
		}(),
		"walletId": func() processwagertransaction.Request {
			r := base
			r.WalletID = uuid.MustParse("00000000-0000-0000-0000-000000000098")
			return r
		}(),
		"roundId": func() processwagertransaction.Request {
			r := base
			r.RoundID = "round-2"
			return r
		}(),
		"gameId": func() processwagertransaction.Request {
			r := base
			r.GameID = "game-2"
			return r
		}(),
		"kind": func() processwagertransaction.Request {
			r := base
			r.Kind = wagertransaction.KindWin
			return r
		}(),
		"amount": func() processwagertransaction.Request {
			r := base
			r.Amount = mustMoney(t, "26.00")
			return r
		}(),
		"referenceExternalTransactionId": func() processwagertransaction.Request {
			r := base
			r.ReferenceExternalTransactionID = "ref-1"
			return r
		}(),
	}

	for name, mutated := range mutations {
		t.Run(name, func(t *testing.T) {
			assert.NotEqual(t, baseHash, processwagertransaction.CanonicalHash(mutated))
		})
	}
}

func withProviderID(req processwagertransaction.Request, providerID string) processwagertransaction.Request {
	req.ProviderID = providerID
	return req
}
