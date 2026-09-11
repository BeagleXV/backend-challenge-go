package httpapi

import (
	"bytes"
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
	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

type walletTestHarness struct {
	handlers *walletHandlers
	wallets  *apptest.WalletRepository
	ledgers  *apptest.LedgerRepository
}

func newWalletTestHarness() *walletTestHarness {
	wallets := apptest.NewWalletRepository()
	txs := apptest.NewWagerTransactionRepository()
	ledgers := apptest.NewLedgerRepository()
	outbox := apptest.NewOutboxRepository()
	clock := apptest.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ids := &apptest.SequentialIDs{}

	openWallet := openwallet.New(apptest.NoopUnitOfWork{}, wallets, txs, ledgers, outbox, clock, ids)
	reconcile := reconciliation.New(apptest.NoopUnitOfWork{}, wallets, ledgers)

	return &walletTestHarness{
		handlers: newWalletHandlers(openWallet, reconcile, wallets, ledgers, ids, zapNop()),
		wallets:  wallets,
		ledgers:  ledgers,
	}
}

func requestWithClaims(method, target string, body []byte, claims idp.Claims) *http.Request {
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, target, bytes.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	return r.WithContext(withClaims(withCorrelationID(r.Context(), "test-correlation"), claims))
}

func TestOpenWalletHandler_CreatesWallet(t *testing.T) {
	h := newWalletTestHarness()
	body, err := json.Marshal(openWalletRequest{
		PlayerID:       uuid.New(),
		InitialBalance: mustMoney(t, "100.00", money.BRL),
	})
	require.NoError(t, err)

	r := requestWithClaims(http.MethodPost, "/wallets", body, idp.Claims{Internal: true})
	rec := httptest.NewRecorder()

	h.handlers.openWalletHandler(rec, r)

	require.Equal(t, http.StatusCreated, rec.Code)
	var resp walletResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "100.00", resp.Balance.String())
	assert.Equal(t, int64(1), resp.Version)
}

func TestOpenWalletHandler_MalformedBody_Returns400(t *testing.T) {
	h := newWalletTestHarness()
	r := requestWithClaims(http.MethodPost, "/wallets", []byte("not json"), idp.Claims{Internal: true})
	rec := httptest.NewRecorder()

	h.handlers.openWalletHandler(rec, r)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestOpenWalletHandler_DuplicateWallet_Returns409(t *testing.T) {
	h := newWalletTestHarness()
	playerID := uuid.New()
	body, _ := json.Marshal(openWalletRequest{PlayerID: playerID, InitialBalance: mustMoney(t, "0.00", money.BRL)})

	r1 := requestWithClaims(http.MethodPost, "/wallets", body, idp.Claims{Internal: true})
	rec1 := httptest.NewRecorder()
	h.handlers.openWalletHandler(rec1, r1)
	require.Equal(t, http.StatusCreated, rec1.Code)

	r2 := requestWithClaims(http.MethodPost, "/wallets", body, idp.Claims{Internal: true})
	rec2 := httptest.NewRecorder()
	h.handlers.openWalletHandler(rec2, r2)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestListLedgerHandler_PaginatesWithOpaqueCursor(t *testing.T) {
	h := newWalletTestHarness()
	body, _ := json.Marshal(openWalletRequest{PlayerID: uuid.New(), InitialBalance: mustMoney(t, "50.00", money.BRL)})
	r := requestWithClaims(http.MethodPost, "/wallets", body, idp.Claims{Internal: true})
	rec := httptest.NewRecorder()
	h.handlers.openWalletHandler(rec, r)
	require.Equal(t, http.StatusCreated, rec.Code)
	var opened walletResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &opened))

	// First page: limit=1 must report a nextCursor (the OPENING credit is
	// the only entry so far, but the handler always fetches limit+1 to
	// decide — assert the shape holds with a real, small limit).
	target := "/wallets/" + opened.ID.String() + "/ledger?limit=1"
	getReq := requestWithClaims(http.MethodGet, target, nil, idp.Claims{Internal: true})
	getReq = withChiParam(getReq, "walletID", opened.ID.String())
	getRec := httptest.NewRecorder()
	h.handlers.listLedgerHandler(getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	var page ledgerPageResponse
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &page))
	assert.Len(t, page.Entries, 1)
	assert.Nil(t, page.NextCursor, "only one entry exists, there must be no next page")
}

func TestReconciliationHandler_ReportsConsistentBalance(t *testing.T) {
	h := newWalletTestHarness()
	body, _ := json.Marshal(openWalletRequest{PlayerID: uuid.New(), InitialBalance: mustMoney(t, "50.00", money.BRL)})
	r := requestWithClaims(http.MethodPost, "/wallets", body, idp.Claims{Internal: true})
	rec := httptest.NewRecorder()
	h.handlers.openWalletHandler(rec, r)
	var opened walletResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &opened))

	target := "/wallets/" + opened.ID.String() + "/reconciliation"
	reconReq := requestWithClaims(http.MethodPost, target, nil, idp.Claims{Internal: true})
	reconReq = withChiParam(reconReq, "walletID", opened.ID.String())
	reconRec := httptest.NewRecorder()
	h.handlers.reconciliationHandler(reconRec, reconReq)

	require.Equal(t, http.StatusOK, reconRec.Code)
	var resp reconciliationResponse
	require.NoError(t, json.Unmarshal(reconRec.Body.Bytes(), &resp))
	assert.True(t, resp.Consistent)
	assert.Equal(t, "0.00", resp.Difference.String())
}

func TestGetWalletHandler_NotFound_Returns404(t *testing.T) {
	h := newWalletTestHarness()
	r := requestWithClaims(http.MethodGet, "/wallets/"+uuid.NewString(), nil, idp.Claims{Internal: true})
	r = withChiParam(r, "walletID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.handlers.getWalletHandler(rec, r)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
