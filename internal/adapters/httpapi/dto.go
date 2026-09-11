package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

// --- wallets ---

type openWalletRequest struct {
	PlayerID       uuid.UUID   `json:"playerId"`
	InitialBalance money.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID       uuid.UUID   `json:"id"`
	PlayerID uuid.UUID   `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int64       `json:"version"`
}

type ledgerEntryResponse struct {
	ID            uuid.UUID   `json:"id"`
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Amount        money.Money `json:"amount"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	CreatedAt     time.Time   `json:"createdAt"`
}

func newLedgerEntryResponse(e *ledger.Entry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID:            e.ID(),
		WalletID:      e.WalletID(),
		TransactionID: e.TransactionID(),
		Direction:     string(e.Direction()),
		Amount:        e.Amount(),
		BalanceBefore: e.BalanceBefore(),
		BalanceAfter:  e.BalanceAfter(),
		CreatedAt:     e.CreatedAt(),
	}
}

type ledgerPageResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor,omitempty"`
}

type reconciliationResponse struct {
	WalletID          uuid.UUID   `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int         `json:"checkedEntries"`
}

// --- wagering transactions ---

type wagerTransactionRequest struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

type wagerTransactionResultResponse struct {
	TransactionID    uuid.UUID    `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          *money.Money `json:"balance,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
	FailureCode      string       `json:"failureCode,omitempty"`
}

type wagerTransactionDetailResponse struct {
	TransactionID                  uuid.UUID    `json:"transactionId"`
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	Status                         string       `json:"status"`
	Kind                           string       `json:"kind"`
	WalletID                       uuid.UUID    `json:"walletId"`
	PlayerID                       uuid.UUID    `json:"playerId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Money                          money.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	CreatedAt                      time.Time    `json:"createdAt"`
	UpdatedAt                      time.Time    `json:"updatedAt"`
}

func newWagerTransactionDetailResponse(tx *wagertransaction.WagerTransaction) wagerTransactionDetailResponse {
	resp := wagerTransactionDetailResponse{
		TransactionID:                  tx.ID(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		Status:                         string(tx.Status()),
		Kind:                           string(tx.Kind()),
		WalletID:                       tx.WalletID(),
		PlayerID:                       tx.PlayerID(),
		RoundID:                        tx.RoundID(),
		GameID:                         tx.GameID(),
		Money:                          tx.Amount(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		FailureCode:                    tx.FailureCode(),
		CreatedAt:                      tx.CreatedAt(),
		UpdatedAt:                      tx.UpdatedAt(),
	}
	if balance, ok := tx.ResultBalance(); ok {
		resp.Balance = &balance
	}
	return resp
}
