package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

// LedgerRepository implements ports.LedgerRepository. Append is the only
// write path — there is deliberately no Update/Delete method, mirroring the
// table's own append-only trigger (Fase 2).
type LedgerRepository struct {
	pool *pgxpool.Pool
}

func NewLedgerRepository(pool *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{pool: pool}
}

func (r *LedgerRepository) Append(ctx context.Context, entry *ledger.Entry) error {
	_, err := q(ctx, r.pool).Exec(ctx,
		`INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		entry.ID(), entry.WalletID(), entry.TransactionID(), string(entry.Direction()),
		entry.Amount().MinorUnits(), string(entry.Amount().Currency()),
		entry.BalanceBefore().MinorUnits(), entry.BalanceAfter().MinorUnits(),
		entry.CreatedAt(),
	)
	if err != nil {
		return mapErr(err, "ledger.Append")
	}
	return nil
}

func (r *LedgerRepository) ListByWallet(ctx context.Context, walletID uuid.UUID) ([]*ledger.Entry, error) {
	rows, err := q(ctx, r.pool).Query(ctx,
		`SELECT id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
		 FROM wallet_ledger_entries
		 WHERE wallet_id = $1
		 ORDER BY created_at`,
		walletID,
	)
	if err != nil {
		return nil, mapErr(err, "ledger.ListByWallet")
	}
	defer rows.Close()

	var out []*ledger.Entry
	for rows.Next() {
		var (
			id, walletRowID, transactionID uuid.UUID
			direction, currency            string
			amount, before, after          int64
			createdAt                      time.Time
		)
		if err := rows.Scan(&id, &walletRowID, &transactionID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
			return nil, mapErr(err, "ledger.ListByWallet")
		}

		cur := money.Currency(currency)
		amountMoney, err := money.FromMinorUnits(amount, cur)
		if err != nil {
			return nil, fmt.Errorf("decode ledger amount: %w", err)
		}
		beforeMoney, err := money.FromMinorUnits(before, cur)
		if err != nil {
			return nil, fmt.Errorf("decode ledger balance_before: %w", err)
		}
		afterMoney, err := money.FromMinorUnits(after, cur)
		if err != nil {
			return nil, fmt.Errorf("decode ledger balance_after: %w", err)
		}

		entry, err := ledger.New(ledger.NewParams{
			ID:            id,
			WalletID:      walletRowID,
			TransactionID: transactionID,
			Direction:     ledger.Direction(direction),
			Amount:        amountMoney,
			BalanceBefore: beforeMoney,
			BalanceAfter:  afterMoney,
			CreatedAt:     createdAt,
		})
		if err != nil {
			return nil, fmt.Errorf("rehydrate ledger entry %s: %w", id, err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, mapErr(err, "ledger.ListByWallet")
	}
	return out, nil
}
