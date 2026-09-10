package wagertransaction_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.New(amount, money.BRL)
	require.NoError(t, err)
	return m
}

func baseExternalParams(t *testing.T, kind wagertransaction.Kind, amount string) wagertransaction.NewExternalParams {
	t.Helper()
	return wagertransaction.NewExternalParams{
		ID:                    uuid.New(),
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PayloadHash:           "deadbeef",
		WalletID:              uuid.New(),
		PlayerID:              uuid.New(),
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  kind,
		Amount:                mustMoney(t, amount),
		Now:                   time.Now(),
	}
}

func TestNewExternal_RejectsOpeningKind(t *testing.T) {
	p := baseExternalParams(t, wagertransaction.KindOpening, "10.00")
	_, err := wagertransaction.NewExternal(p)
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrInvalidKind))
}

func TestNewExternal_ZeroValuePolicyPerKind(t *testing.T) {
	cases := []struct {
		kind      wagertransaction.Kind
		amount    string
		wantError bool
	}{
		{wagertransaction.KindBet, "0.00", true},
		{wagertransaction.KindBet, "25.00", false},
		{wagertransaction.KindWin, "0.00", true},
		{wagertransaction.KindWin, "25.00", false},
		{wagertransaction.KindLoss, "0.00", false},
		{wagertransaction.KindLoss, "0.01", true},
		{wagertransaction.KindRefund, "0.00", true},
		{wagertransaction.KindRollback, "0.00", true},
	}
	for _, c := range cases {
		t.Run(string(c.kind)+"_"+c.amount, func(t *testing.T) {
			p := baseExternalParams(t, c.kind, c.amount)
			if c.kind == wagertransaction.KindRefund || c.kind == wagertransaction.KindRollback {
				p.ReferenceExternalTransactionID = "transaction-ref"
			}
			_, err := wagertransaction.NewExternal(p)
			if c.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewExternal_ReversalsRequireReference(t *testing.T) {
	for _, kind := range []wagertransaction.Kind{wagertransaction.KindRefund, wagertransaction.KindRollback} {
		t.Run(string(kind), func(t *testing.T) {
			p := baseExternalParams(t, kind, "25.00")
			_, err := wagertransaction.NewExternal(p)
			require.Error(t, err)
			require.True(t, errors.Is(err, wagertransaction.ErrReferenceRequired))
		})
	}
}

func TestNewExternal_NonReversalsRejectReference(t *testing.T) {
	p := baseExternalParams(t, wagertransaction.KindBet, "25.00")
	p.ReferenceExternalTransactionID = "should-not-be-here"
	_, err := wagertransaction.NewExternal(p)
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrReferenceNotApplicable))
}

func TestNewExternal_RequiresProviderMetadata(t *testing.T) {
	p := baseExternalParams(t, wagertransaction.KindBet, "25.00")
	p.ProviderID = ""
	_, err := wagertransaction.NewExternal(p)
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrInvalidWagerTransaction))
}

func TestNewExternal_StartsInPending(t *testing.T) {
	p := baseExternalParams(t, wagertransaction.KindBet, "25.00")
	tx, err := wagertransaction.NewExternal(p)
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusPending, tx.Status())
	require.Equal(t, wagertransaction.OriginExternal, tx.Origin())
}

func TestNewInternalOpening_AllowsZeroOrPositive(t *testing.T) {
	_, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID:       uuid.New(),
		WalletID: uuid.New(),
		PlayerID: uuid.New(),
		Amount:   mustMoney(t, "0.00"),
		Now:      time.Now(),
	})
	require.NoError(t, err)

	_, err = wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID:       uuid.New(),
		WalletID: uuid.New(),
		PlayerID: uuid.New(),
		Amount:   mustMoney(t, "1000.00"),
		Now:      time.Now(),
	})
	require.NoError(t, err)
}

func TestNewInternalOpening_RejectsNegative(t *testing.T) {
	negative, err := money.New("-1.00", money.BRL)
	require.NoError(t, err)

	_, err = wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID:       uuid.New(),
		WalletID: uuid.New(),
		PlayerID: uuid.New(),
		Amount:   negative,
		Now:      time.Now(),
	})
	require.Error(t, err)
}

func TestInternalOpening_HasNoExternalMetadata(t *testing.T) {
	tx, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
		ID:       uuid.New(),
		WalletID: uuid.New(),
		PlayerID: uuid.New(),
		Amount:   mustMoney(t, "1000.00"),
		Now:      time.Now(),
	})
	require.NoError(t, err)
	require.Equal(t, wagertransaction.OriginInternal, tx.Origin())
	require.Equal(t, wagertransaction.KindOpening, tx.Kind())
	require.Empty(t, tx.ProviderID())
	require.Empty(t, tx.ExternalTransactionID())
	require.Empty(t, tx.IdempotencyKey())
	require.Empty(t, tx.PayloadHash())
	require.Empty(t, tx.RoundID())
	require.Empty(t, tx.GameID())
}

func newPendingTx(t *testing.T) *wagertransaction.WagerTransaction {
	t.Helper()
	p := baseExternalParams(t, wagertransaction.KindBet, "25.00")
	tx, err := wagertransaction.NewExternal(p)
	require.NoError(t, err)
	return tx
}

func TestTransitions_ValidPaths(t *testing.T) {
	t.Run("PENDING to PROCESSED", func(t *testing.T) {
		tx := newPendingTx(t)
		require.NoError(t, tx.MarkProcessed(time.Now()))
		require.Equal(t, wagertransaction.StatusProcessed, tx.Status())
	})

	t.Run("PENDING to REJECTED", func(t *testing.T) {
		tx := newPendingTx(t)
		require.NoError(t, tx.MarkRejected("INSUFFICIENT_BALANCE", time.Now()))
		require.Equal(t, wagertransaction.StatusRejected, tx.Status())
		require.Equal(t, "INSUFFICIENT_BALANCE", tx.FailureCode())
	})

	t.Run("PENDING to FAILED", func(t *testing.T) {
		tx := newPendingTx(t)
		require.NoError(t, tx.MarkFailed("DB_UNAVAILABLE", time.Now()))
		require.Equal(t, wagertransaction.StatusFailed, tx.Status())
	})

	t.Run("PENDING to PENDING_REFERENCE to PROCESSED", func(t *testing.T) {
		tx := newPendingTx(t)
		require.NoError(t, tx.MarkPendingReference(time.Now()))
		require.Equal(t, wagertransaction.StatusPendingReference, tx.Status())
		require.NoError(t, tx.MarkProcessed(time.Now()))
		require.Equal(t, wagertransaction.StatusProcessed, tx.Status())
	})

	t.Run("PENDING_REFERENCE to REJECTED", func(t *testing.T) {
		tx := newPendingTx(t)
		require.NoError(t, tx.MarkPendingReference(time.Now()))
		require.NoError(t, tx.MarkRejected("REFERENCE_NOT_FOUND", time.Now()))
		require.Equal(t, wagertransaction.StatusRejected, tx.Status())
	})
}

func TestTransitions_TerminalStatesRejectFurtherTransitions(t *testing.T) {
	terminalSetups := map[string]func(t *testing.T) *wagertransaction.WagerTransaction{
		"PROCESSED": func(t *testing.T) *wagertransaction.WagerTransaction {
			tx := newPendingTx(t)
			require.NoError(t, tx.MarkProcessed(time.Now()))
			return tx
		},
		"REJECTED": func(t *testing.T) *wagertransaction.WagerTransaction {
			tx := newPendingTx(t)
			require.NoError(t, tx.MarkRejected("CODE", time.Now()))
			return tx
		},
		"FAILED": func(t *testing.T) *wagertransaction.WagerTransaction {
			tx := newPendingTx(t)
			require.NoError(t, tx.MarkFailed("CODE", time.Now()))
			return tx
		},
	}

	for name, setup := range terminalSetups {
		t.Run(name, func(t *testing.T) {
			tx := setup(t)
			require.True(t, tx.Status().IsTerminal())

			err := tx.MarkProcessed(time.Now())
			require.Error(t, err)
			require.True(t, errors.Is(err, wagertransaction.ErrInvalidTransition))

			err = tx.MarkPendingReference(time.Now())
			require.Error(t, err)
			require.True(t, errors.Is(err, wagertransaction.ErrInvalidTransition))
		})
	}
}

func TestTransitions_InvalidDirectPendingReferenceToPendingReference(t *testing.T) {
	tx := newPendingTx(t)
	require.NoError(t, tx.MarkPendingReference(time.Now()))
	err := tx.MarkPendingReference(time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrInvalidTransition))
}

func TestMarkRejected_RequiresFailureCode(t *testing.T) {
	tx := newPendingTx(t)
	err := tx.MarkRejected("", time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrFailureCodeRequired))
}

func TestResolveReference_OnlyForReversals(t *testing.T) {
	tx := newPendingTx(t) // a BET
	err := tx.ResolveReference(uuid.New(), time.Now())
	require.Error(t, err)
	require.True(t, errors.Is(err, wagertransaction.ErrReferenceNotApplicable))
}

func TestResolveReference_ForRefund(t *testing.T) {
	p := baseExternalParams(t, wagertransaction.KindRefund, "25.00")
	p.ReferenceExternalTransactionID = "bet-1"
	tx, err := wagertransaction.NewExternal(p)
	require.NoError(t, err)

	resolvedID := uuid.New()
	require.NoError(t, tx.ResolveReference(resolvedID, time.Now()))
	require.Equal(t, resolvedID, tx.ResolvedReferenceID())
}

func TestRehydrate_PreservesTerminalStateWithoutValidation(t *testing.T) {
	tx, err := wagertransaction.Rehydrate(wagertransaction.RehydrateParams{
		ID:        uuid.New(),
		Origin:    wagertransaction.OriginExternal,
		Kind:      wagertransaction.KindBet,
		Status:    wagertransaction.StatusProcessed,
		WalletID:  uuid.New(),
		PlayerID:  uuid.New(),
		Amount:    mustMoney(t, "25.00"),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	require.NoError(t, err)
	require.Equal(t, wagertransaction.StatusProcessed, tx.Status())

	// rehydrated terminal state still rejects new transitions
	err = tx.MarkProcessed(time.Now())
	require.Error(t, err)
}
