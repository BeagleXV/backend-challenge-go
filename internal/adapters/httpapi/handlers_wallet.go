package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/beaglexv/backend-challenge-go/internal/application/openwallet"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/reconciliation"
)

const defaultLedgerPageLimit = 50
const maxLedgerPageLimit = 200

// walletHandlers groups every /wallets* handler. Reads (GetByID,
// ListByWalletPage) go straight to the repository ports — there is no
// domain invariant to protect on a read, so a dedicated use case would add
// nothing over calling the port directly.
type walletHandlers struct {
	openWallet *openwallet.Service
	reconcile  *reconciliation.Service
	wallets    ports.WalletRepository
	ledgers    ports.LedgerRepository
	idGen      ports.IDGenerator
	logger     *zap.Logger
}

func newWalletHandlers(
	openWallet *openwallet.Service,
	reconcile *reconciliation.Service,
	wallets ports.WalletRepository,
	ledgers ports.LedgerRepository,
	idGen ports.IDGenerator,
	logger *zap.Logger,
) *walletHandlers {
	return &walletHandlers{
		openWallet: openWallet,
		reconcile:  reconcile,
		wallets:    wallets,
		ledgers:    ledgers,
		idGen:      idGen,
		logger:     logger,
	}
}

func (h *walletHandlers) openWalletHandler(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "malformed JSON body")
		return
	}

	result, err := h.openWallet.Handle(r.Context(), openwallet.Request{
		WalletID:       h.idGen.NewID(),
		PlayerID:       req.PlayerID,
		Currency:       req.InitialBalance.Currency(),
		InitialBalance: req.InitialBalance,
		CorrelationID:  correlationIDFromContext(r.Context()),
	})
	if err != nil {
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "open_wallet", err)
		return
	}

	writeJSON(w, http.StatusCreated, walletResponse{
		ID:       result.WalletID,
		PlayerID: req.PlayerID,
		Balance:  result.Balance,
		Version:  result.Version,
	})
}

func (h *walletHandlers) getWalletHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(chi.URLParam(r, "walletID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "walletId must be a valid UUID")
		return
	}

	wallet, err := h.wallets.GetByID(r.Context(), walletID)
	if err != nil {
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "get_wallet", err)
		return
	}

	writeJSON(w, http.StatusOK, walletResponse{
		ID:       wallet.ID(),
		PlayerID: wallet.PlayerID(),
		Balance:  wallet.Balance(),
		Version:  wallet.Version(),
	})
}

func (h *walletHandlers) listLedgerHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(chi.URLParam(r, "walletID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "walletId must be a valid UUID")
		return
	}

	limit := defaultLedgerPageLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := parsePositiveInt(raw)
		if err != nil || parsed > maxLedgerPageLimit {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "limit must be a positive integer up to 200")
			return
		}
		limit = parsed
	}

	var after ledgerCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		after, err = decodeLedgerCursor(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid cursor")
			return
		}
	}

	// Fetch one extra row to know whether another page follows, without
	// exposing that lookahead row itself.
	entries, err := h.ledgers.ListByWalletPage(r.Context(), walletID, after.CreatedAt, after.ID, limit+1)
	if err != nil {
		writeUnexpectedError(w, r, h.logger, "list_ledger", err)
		return
	}

	resp := ledgerPageResponse{Entries: make([]ledgerEntryResponse, 0, len(entries))}
	for i, e := range entries {
		if i == limit {
			next, err := encodeLedgerCursor(ledgerCursor{CreatedAt: entries[limit-1].CreatedAt(), ID: entries[limit-1].ID()})
			if err != nil {
				writeUnexpectedError(w, r, h.logger, "list_ledger", err)
				return
			}
			resp.NextCursor = &next
			break
		}
		resp.Entries = append(resp.Entries, newLedgerEntryResponse(e))
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *walletHandlers) reconciliationHandler(w http.ResponseWriter, r *http.Request) {
	walletID, err := uuid.Parse(chi.URLParam(r, "walletID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "walletId must be a valid UUID")
		return
	}

	result, err := h.reconcile.Reconcile(r.Context(), walletID)
	if err != nil {
		if writeApplicationError(w, err) {
			return
		}
		writeUnexpectedError(w, r, h.logger, "reconciliation", err)
		return
	}

	if !result.Consistent {
		h.logger.Warn("reconciliation_divergence",
			zap.String("walletId", walletID.String()),
			zap.String("storedBalance", result.StoredBalance.String()),
			zap.String("calculatedBalance", result.CalculatedBalance.String()),
			zap.String("difference", result.Difference.String()),
		)
	}

	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID:          result.WalletID,
		StoredBalance:     result.StoredBalance,
		CalculatedBalance: result.CalculatedBalance,
		Difference:        result.Difference,
		Consistent:        result.Consistent,
		CheckedEntries:    result.CheckedEntries,
	})
}
