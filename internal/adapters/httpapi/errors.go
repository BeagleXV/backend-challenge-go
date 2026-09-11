package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeError writes a {"error":{"code","message"}} body. message is
// intended for the caller (a provider integrating against this API), so it
// must never include internal detail (a raw SQL error, a stack trace) —
// callers that need to log that detail do so separately, server-side,
// before calling writeError.
func writeError(w http.ResponseWriter, status int, code, message string) {
	body := errorBody{}
	body.Error.Code = code
	body.Error.Message = message
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeUnexpectedError logs the real error (server-side only — it may
// contain internal detail) and responds with a generic 500. Nothing about
// err ever reaches the client.
func writeUnexpectedError(w http.ResponseWriter, r *http.Request, logger *zap.Logger, op string, err error) {
	logger.Error(op,
		zap.Error(err),
		zap.String("correlationId", correlationIDFromContext(r.Context())),
	)
	writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
}

// writeApplicationError maps an error returned by a use case or a ports
// repository to the HTTP status/code the contract requires. It returns
// false when err isn't one of the recognized application errors, so the
// caller falls back to writeUnexpectedError.
func writeApplicationError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, ports.ErrAlreadyExists), errors.Is(err, openwallet.ErrWalletAlreadyExists):
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "resource already exists")
	case errors.Is(err, openwallet.ErrInvalidRequest),
		errors.Is(err, processwagertransaction.ErrInvalidRequest),
		errors.Is(err, wagertransaction.ErrInvalidWagerTransaction),
		errors.Is(err, wagertransaction.ErrInvalidKind),
		errors.Is(err, wagertransaction.ErrReferenceRequired),
		errors.Is(err, wagertransaction.ErrReferenceNotApplicable):
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, processwagertransaction.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency key reused with a different payload")
	case errors.Is(err, processwagertransaction.ErrExternalIDReused):
		writeError(w, http.StatusConflict, "EXTERNAL_ID_REUSED", "operation already exists under a different idempotency key")
	case errors.Is(err, processwagertransaction.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found")
	default:
		return false
	}
	return true
}
