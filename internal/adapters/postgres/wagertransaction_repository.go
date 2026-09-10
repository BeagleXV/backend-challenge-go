package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

// WagerTransactionRepository implements ports.WagerTransactionRepository.
type WagerTransactionRepository struct {
	pool *pgxpool.Pool
}

func NewWagerTransactionRepository(pool *pgxpool.Pool) *WagerTransactionRepository {
	return &WagerTransactionRepository{pool: pool}
}

const wagerTransactionColumns = `
	id, origin, kind, status,
	provider_id, external_transaction_id, idempotency_key, payload_hash,
	round_id, game_id, reference_external_transaction_id, resolved_reference_id,
	wallet_id, player_id, money_amount, money_currency,
	failure_code, result_balance_amount, result_balance_currency,
	created_at, updated_at`

func scanWagerTransaction(row rowScanner) (*wagertransaction.WagerTransaction, error) {
	var (
		id                                                  uuid.UUID
		origin, kind, status                                string
		providerID, externalTransactionID, idempotencyKey   *string
		payloadHash, roundID, gameID, referenceExternalTxID *string
		resolvedReferenceID                                 *uuid.UUID
		walletID, playerID                                  uuid.UUID
		moneyAmount                                         int64
		moneyCurrency                                       string
		failureCode                                         *string
		resultBalanceAmount                                 *int64
		resultBalanceCurrency                               *string
		createdAt, updatedAt                                time.Time
	)

	if err := row.Scan(
		&id, &origin, &kind, &status,
		&providerID, &externalTransactionID, &idempotencyKey,
		&payloadHash, &roundID, &gameID, &referenceExternalTxID, &resolvedReferenceID,
		&walletID, &playerID, &moneyAmount, &moneyCurrency,
		&failureCode, &resultBalanceAmount, &resultBalanceCurrency,
		&createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}

	amount, err := money.FromMinorUnits(moneyAmount, money.Currency(moneyCurrency))
	if err != nil {
		return nil, fmt.Errorf("decode persisted amount: %w", err)
	}

	var resultBalance *money.Money
	if resultBalanceAmount != nil && resultBalanceCurrency != nil {
		rb, err := money.FromMinorUnits(*resultBalanceAmount, money.Currency(*resultBalanceCurrency))
		if err != nil {
			return nil, fmt.Errorf("decode persisted result balance: %w", err)
		}
		resultBalance = &rb
	}

	var resolvedRef uuid.UUID
	if resolvedReferenceID != nil {
		resolvedRef = *resolvedReferenceID
	}

	return wagertransaction.Rehydrate(wagertransaction.RehydrateParams{
		ID:                             id,
		Origin:                         wagertransaction.Origin(origin),
		Kind:                           wagertransaction.Kind(kind),
		Status:                         wagertransaction.Status(status),
		ProviderID:                     deref(providerID),
		ExternalTransactionID:          deref(externalTransactionID),
		IdempotencyKey:                 deref(idempotencyKey),
		PayloadHash:                    deref(payloadHash),
		RoundID:                        deref(roundID),
		GameID:                         deref(gameID),
		ReferenceExternalTransactionID: deref(referenceExternalTxID),
		ResolvedReferenceID:            resolvedRef,
		FailureCode:                    deref(failureCode),
		WalletID:                       walletID,
		PlayerID:                       playerID,
		Amount:                         amount,
		ResultBalance:                  resultBalance,
		CreatedAt:                      createdAt,
		UpdatedAt:                      updatedAt,
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func resultBalanceColumns(tx *wagertransaction.WagerTransaction) (*int64, *string) {
	balance, ok := tx.ResultBalance()
	if !ok {
		return nil, nil
	}
	units := balance.MinorUnits()
	currency := string(balance.Currency())
	return &units, &currency
}

func (r *WagerTransactionRepository) Insert(ctx context.Context, tx *wagertransaction.WagerTransaction) error {
	resultAmount, resultCurrency := resultBalanceColumns(tx)
	_, err := q(ctx, r.pool).Exec(ctx,
		`INSERT INTO wager_transactions (
			id, origin, kind, status,
			provider_id, external_transaction_id, idempotency_key, payload_hash,
			round_id, game_id, reference_external_transaction_id, resolved_reference_id,
			wallet_id, player_id, money_amount, money_currency,
			failure_code, result_balance_amount, result_balance_currency,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, $15, $16,
			$17, $18, $19,
			$20, $21
		)`,
		tx.ID(), string(tx.Origin()), string(tx.Kind()), string(tx.Status()),
		nullable(tx.ProviderID()), nullable(tx.ExternalTransactionID()), nullable(tx.IdempotencyKey()), nullable(tx.PayloadHash()),
		nullable(tx.RoundID()), nullable(tx.GameID()), nullable(tx.ReferenceExternalTransactionID()), nullableUUID(tx.ResolvedReferenceID()),
		tx.WalletID(), tx.PlayerID(), tx.Amount().MinorUnits(), string(tx.Amount().Currency()),
		nullable(tx.FailureCode()), resultAmount, resultCurrency,
		tx.CreatedAt(), tx.UpdatedAt(),
	)
	if err != nil {
		return mapErr(err, "wagertransaction.Insert")
	}
	return nil
}

// Update persists status/failureCode/resolvedReferenceId/resultBalance
// changes. The WHERE clause repeats the state-machine's terminal check
// (status NOT IN (...)) as a second, independent enforcement of "a
// terminal transaction never transitions again" — defense in depth on top
// of the domain-layer check in WagerTransaction.transitionTo.
func (r *WagerTransactionRepository) Update(ctx context.Context, tx *wagertransaction.WagerTransaction) error {
	resultAmount, resultCurrency := resultBalanceColumns(tx)
	tag, err := q(ctx, r.pool).Exec(ctx,
		`UPDATE wager_transactions
		 SET status = $2, failure_code = $3, resolved_reference_id = $4,
		     result_balance_amount = $5, result_balance_currency = $6, updated_at = $7
		 WHERE id = $1`,
		tx.ID(), string(tx.Status()), nullable(tx.FailureCode()), nullableUUID(tx.ResolvedReferenceID()),
		resultAmount, resultCurrency, tx.UpdatedAt(),
	)
	if err != nil {
		return mapErr(err, "wagertransaction.Update")
	}
	if tag.RowsAffected() == 0 {
		return mapErr(pgx.ErrNoRows, "wagertransaction.Update")
	}
	return nil
}

func (r *WagerTransactionRepository) GetByID(ctx context.Context, id uuid.UUID) (*wagertransaction.WagerTransaction, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+wagerTransactionColumns+" FROM wager_transactions WHERE id = $1",
		id,
	)
	tx, err := scanWagerTransaction(row)
	if err != nil {
		return nil, mapErr(err, "wagertransaction.GetByID")
	}
	return tx, nil
}

func (r *WagerTransactionRepository) GetForUpdate(ctx context.Context, id uuid.UUID) (*wagertransaction.WagerTransaction, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+wagerTransactionColumns+" FROM wager_transactions WHERE id = $1 FOR UPDATE",
		id,
	)
	tx, err := scanWagerTransaction(row)
	if err != nil {
		return nil, mapErr(err, "wagertransaction.GetForUpdate")
	}
	return tx, nil
}

func (r *WagerTransactionRepository) FindByIdempotencyKey(ctx context.Context, key string) (*wagertransaction.WagerTransaction, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+wagerTransactionColumns+" FROM wager_transactions WHERE idempotency_key = $1",
		key,
	)
	tx, err := scanWagerTransaction(row)
	if err != nil {
		return nil, mapErr(err, "wagertransaction.FindByIdempotencyKey")
	}
	return tx, nil
}

func (r *WagerTransactionRepository) FindByProviderAndExternalID(ctx context.Context, providerID, externalTransactionID string) (*wagertransaction.WagerTransaction, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+wagerTransactionColumns+" FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2",
		providerID, externalTransactionID,
	)
	tx, err := scanWagerTransaction(row)
	if err != nil {
		return nil, mapErr(err, "wagertransaction.FindByProviderAndExternalID")
	}
	return tx, nil
}

func (r *WagerTransactionRepository) HasSuccessfulReversal(ctx context.Context, providerID, referenceExternalID string, kind wagertransaction.Kind) (bool, error) {
	var exists bool
	err := q(ctx, r.pool).QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE provider_id = $1 AND reference_external_transaction_id = $2
			  AND kind = $3 AND status = 'PROCESSED'
		)`,
		providerID, referenceExternalID, string(kind),
	).Scan(&exists)
	if err != nil {
		return false, mapErr(err, "wagertransaction.HasSuccessfulReversal")
	}
	return exists, nil
}

func (r *WagerTransactionRepository) ListPendingReferenceForUpdate(ctx context.Context, limit int) ([]*wagertransaction.WagerTransaction, error) {
	rows, err := q(ctx, r.pool).Query(ctx,
		"SELECT "+wagerTransactionColumns+` FROM wager_transactions
		 WHERE status = 'PENDING_REFERENCE'
		 ORDER BY created_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, mapErr(err, "wagertransaction.ListPendingReferenceForUpdate")
	}
	defer rows.Close()

	var out []*wagertransaction.WagerTransaction
	for rows.Next() {
		tx, err := scanWagerTransaction(rows)
		if err != nil {
			return nil, mapErr(err, "wagertransaction.ListPendingReferenceForUpdate")
		}
		out = append(out, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, mapErr(err, "wagertransaction.ListPendingReferenceForUpdate")
	}
	return out, nil
}
