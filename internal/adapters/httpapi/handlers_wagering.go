package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/platform/metrics"
)

type wageringHandlers struct {
	processor *processwagertransaction.Service
	txs       ports.WagerTransactionRepository
	metrics   *metrics.Metrics
	logger    *zap.Logger
}

func newWageringHandlers(processor *processwagertransaction.Service, txs ports.WagerTransactionRepository, m *metrics.Metrics, logger *zap.Logger) *wageringHandlers {
	return &wageringHandlers{processor: processor, txs: txs, metrics: m, logger: logger}
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

	start := time.Now()
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
	h.metrics.RecordProcessingDuration(r.Context(), metrics.TransportHTTP, time.Since(start))
	if err != nil {
		recordConflictMetric(r.Context(), h.metrics, err)
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "wagering_submit", err)
		return
	}
	h.metrics.RecordWagerTransaction(r.Context(), string(result.Status), metrics.TransportHTTP, result.IdempotentReplay)
	h.logger.Info("wagering_submit_processed",
		zap.String("correlationId", correlationIDFromContext(r.Context())),
		zap.String("transactionId", result.TransactionID.String()),
		zap.String("walletId", req.WalletID.String()),
		zap.String("providerId", req.ProviderID),
		zap.String("status", string(result.Status)),
		zap.Bool("idempotentReplay", result.IdempotentReplay),
	)

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

// recordConflictMetric records concurrency_conflicts_total for the two
// conflict errors Handle can return — both are exactly the "two callers
// raced" case the metric exists for; every other error either isn't a
// conflict (validation, not-found) or, in the case of the transient
// insert-race Handle already retries internally, never escapes to a
// caller to be recorded here at all.
func recordConflictMetric(ctx context.Context, m *metrics.Metrics, err error) {
	switch {
	case errors.Is(err, processwagertransaction.ErrIdempotencyConflict):
		m.RecordConcurrencyConflict(ctx, metrics.ConflictReasonIdempotencyConflict)
	case errors.Is(err, processwagertransaction.ErrExternalIDReused):
		m.RecordConcurrencyConflict(ctx, metrics.ConflictReasonExternalIDReused)
	}
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
