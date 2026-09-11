package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

type wageringHandlers struct {
	processor *processwagertransaction.Service
	txs       ports.WagerTransactionRepository
	logger    *zap.Logger
}

func newWageringHandlers(processor *processwagertransaction.Service, txs ports.WagerTransactionRepository, logger *zap.Logger) *wageringHandlers {
	return &wageringHandlers{processor: processor, txs: txs, logger: logger}
}

// statusHTTPCode maps a successfully-returned (err == nil) processing
// result's terminal/interim status to the HTTP status the contract
// requires: 200 for a completed operation, 202 for one still waiting on a
// reference, 422 for a definitive business rejection. FAILED is not
// expected on this synchronous path (it is set only by the pending-
// reference worker after exhausting retries) but is mapped defensively.
func statusHTTPCode(status wagertransaction.Status) int {
	switch status {
	case wagertransaction.StatusProcessed:
		return http.StatusOK
	case wagertransaction.StatusPendingReference:
		return http.StatusAccepted
	case wagertransaction.StatusRejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func (h *wageringHandlers) submitHandler(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "Idempotency-Key header is required")
		return
	}

	var req wagerTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "malformed JSON body")
		return
	}

	if req.ProviderID != claimsFromContext(r.Context()).ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "providerId does not match the authenticated caller")
		return
	}
	if req.Kind == string(wagertransaction.KindOpening) {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "OPENING is reserved for internal wallet creation")
		return
	}

	result, err := h.processor.Handle(r.Context(), processwagertransaction.Request{
		IdempotencyKey:                 idempotencyKey,
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		WalletID:                       req.WalletID,
		PlayerID:                       req.PlayerID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           wagertransaction.Kind(req.Kind),
		Amount:                         req.Money,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID:                  correlationIDFromContext(r.Context()),
	})
	if err != nil {
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "wagering_submit", err)
		return
	}

	resp := wagerTransactionResultResponse{
		TransactionID:    result.TransactionID,
		Status:           string(result.Status),
		IdempotentReplay: result.IdempotentReplay,
		FailureCode:      result.FailureCode,
	}
	if result.HasBalance {
		resp.Balance = &result.Balance
	}
	writeJSON(w, statusHTTPCode(result.Status), resp)
}

func (h *wageringHandlers) getByIDHandler(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(chi.URLParam(r, "transactionID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "transactionId must be a valid UUID")
		return
	}

	tx, err := h.txs.GetByID(r.Context(), transactionID)
	if err != nil {
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "wagering_get_by_id", err)
		return
	}

	// A provider must never learn that another provider's transaction id
	// exists — 404, not 403, on a mismatch.
	if tx.ProviderID() != claimsFromContext(r.Context()).ProviderID {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}

	writeJSON(w, http.StatusOK, newWagerTransactionDetailResponse(tx))
}

func (h *wageringHandlers) getByProviderAndExternalIDHandler(w http.ResponseWriter, r *http.Request) {
	providerID := chi.URLParam(r, "providerID")
	externalID := chi.URLParam(r, "externalTransactionID")

	if providerID != claimsFromContext(r.Context()).ProviderID {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}

	tx, err := h.txs.FindByProviderAndExternalID(r.Context(), providerID, externalID)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
			return
		}
		writeUnexpectedError(w, r, h.logger, "wagering_get_by_provider_external_id", err)
		return
	}

	writeJSON(w, http.StatusOK, newWagerTransactionDetailResponse(tx))
}
