package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/idp"
	"github.com/beaglexv/backend-challenge-go/internal/application/apptest"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

type wageringTestHarness struct {
	handlers *wageringHandlers
	wallets  *apptest.WalletRepository
	txs      *apptest.WagerTransactionRepository
}

func newWageringTestHarness() *wageringTestHarness {
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	inbox := apptest.NewInboxRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	processor := processwagertransaction.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, inbox, outbox, clock, ids)

	return &wageringTestHarness{
		handlers: newWageringHandlers(processor, txs, zapNop()),
		wallets:  wallets,
		txs:      txs,
	}
}

func (h *wageringTestHarness) seedWallet(t *testing.T, balance string) uuid.UUID {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID:             uuid.New(),
		PlayerID:       uuid.New(),
		Currency:       money.BRL,
		InitialBalance: mustMoney(t, balance, money.BRL),
		Now:            time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, h.wallets.Insert(context.Background(), w))
	return w.ID()
}

func submitRequest(t *testing.T, walletID uuid.UUID, providerID, externalID, kind, amount, idempotencyKey string) *http.Request {
	t.Helper()
	body, err := json.Marshal(wagerTransactionRequest{
		ProviderID:            providerID,
		ExternalTransactionID: externalID,
		PlayerID:              uuid.New(),
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Money:                 mustMoney(t, amount, money.BRL),
	})
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/wagering/transactions", bytes.NewReader(body))
	if idempotencyKey != "" {
		r.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return r.WithContext(withClaims(withCorrelationID(r.Context(), "test"), idp.Claims{ProviderID: providerID}))
}

func TestSubmitHandler_MissingIdempotencyKey_Returns400(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	r := submitRequest(t, walletID, "provider-a", "tx-1", "BET", "25.00", "")
	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSubmitHandler_ProviderMismatch_Returns403(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	body, _ := json.Marshal(wagerTransactionRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "tx-1",
		PlayerID:              uuid.New(),
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 mustMoney(t, "25.00", money.BRL),
	})
	r := httptest.NewRequest(http.MethodPost, "/wagering/transactions", bytes.NewReader(body))
	r.Header.Set("Idempotency-Key", "provider-a:tx-1")
	// authenticated as provider-b, submitting on behalf of provider-a
	r = r.WithContext(withClaims(withCorrelationID(r.Context(), "test"), idp.Claims{ProviderID: "provider-b"}))

	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestSubmitHandler_RejectsOpeningKind(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	r := submitRequest(t, walletID, "provider-a", "tx-1", "OPENING", "25.00", "provider-a:tx-1")
	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSubmitHandler_Bet_Processed200(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	r := submitRequest(t, walletID, "provider-a", "tx-1", "BET", "25.00", "provider-a:tx-1")
	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp wagerTransactionResultResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "PROCESSED", resp.Status)
	require.NotNil(t, resp.Balance)
	assert.Equal(t, "75.00", resp.Balance.String())
	assert.False(t, resp.IdempotentReplay)
}

func TestSubmitHandler_InsufficientBalance_Returns422(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "10.00")

	r := submitRequest(t, walletID, "provider-a", "tx-1", "BET", "25.00", "provider-a:tx-1")
	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	var resp wagerTransactionResultResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "REJECTED", resp.Status)
	assert.Equal(t, processwagertransaction.FailureCodeInsufficientBalance, resp.FailureCode)
}

func TestSubmitHandler_IdempotentReplay(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	body, err := json.Marshal(wagerTransactionRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "tx-1",
		PlayerID:              uuid.New(),
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 mustMoney(t, "25.00", money.BRL),
	})
	require.NoError(t, err)
	newReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/wagering/transactions", bytes.NewReader(body))
		r.Header.Set("Idempotency-Key", "provider-a:tx-1")
		return r.WithContext(withClaims(withCorrelationID(r.Context(), "test"), idp.Claims{ProviderID: "provider-a"}))
	}

	rec1 := httptest.NewRecorder()
	h.handlers.submitHandler(rec1, newReq())
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := httptest.NewRecorder()
	h.handlers.submitHandler(rec2, newReq())
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp wagerTransactionResultResponse
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	assert.True(t, resp.IdempotentReplay)
}

func TestGetByIDHandler_OtherProvidersTransaction_Returns404(t *testing.T) {
	h := newWageringTestHarness()
	walletID := h.seedWallet(t, "100.00")

	r := submitRequest(t, walletID, "provider-a", "tx-1", "BET", "25.00", "provider-a:tx-1")
	rec := httptest.NewRecorder()
	h.handlers.submitHandler(rec, r)
	require.Equal(t, http.StatusOK, rec.Code)
	var submitted wagerTransactionResultResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &submitted))

	getReq := httptest.NewRequest(http.MethodGet, "/wagering/transactions/"+submitted.TransactionID.String(), nil)
	getReq = getReq.WithContext(withClaims(getReq.Context(), idp.Claims{ProviderID: "provider-b"}))
	getReq = withChiParam(getReq, "transactionID", submitted.TransactionID.String())
	getRec := httptest.NewRecorder()
	h.handlers.getByIDHandler(getRec, getReq)

	assert.Equal(t, http.StatusNotFound, getRec.Code)
}
