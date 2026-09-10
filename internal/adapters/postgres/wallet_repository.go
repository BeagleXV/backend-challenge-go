package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

// WalletRepository implements ports.WalletRepository.
type WalletRepository struct {
	pool *pgxpool.Pool
}

func NewWalletRepository(pool *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{pool: pool}
}

const walletColumns = "id, player_id, currency, balance, version, created_at, updated_at"

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// which is all scanWallet needs.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanWallet(row rowScanner) (*wallet.Wallet, error) {
	var (
		id, playerID       uuid.UUID
		currency           string
		balanceMinorUnits  int64
		version            int64
		createdAt, updated time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balanceMinorUnits, &version, &createdAt, &updated); err != nil {
		return nil, err
	}

	balance, err := money.FromMinorUnits(balanceMinorUnits, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("decode persisted balance: %w", err)
	}

	return wallet.Rehydrate(wallet.RehydrateParams{
		ID:        id,
		PlayerID:  playerID,
		Currency:  money.Currency(currency),
		Balance:   balance,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updated,
	})
}

func (r *WalletRepository) GetForUpdate(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+walletColumns+" FROM wallets WHERE id = $1 FOR UPDATE",
		walletID,
	)
	w, err := scanWallet(row)
	if err != nil {
		return nil, mapErr(err, "wallet.GetForUpdate")
	}
	return w, nil
}

func (r *WalletRepository) GetByID(ctx context.Context, walletID uuid.UUID) (*wallet.Wallet, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+walletColumns+" FROM wallets WHERE id = $1",
		walletID,
	)
	w, err := scanWallet(row)
	if err != nil {
		return nil, mapErr(err, "wallet.GetByID")
	}
	return w, nil
}

func (r *WalletRepository) GetByPlayerAndCurrency(ctx context.Context, playerID uuid.UUID, currency money.Currency) (*wallet.Wallet, error) {
	row := q(ctx, r.pool).QueryRow(ctx,
		"SELECT "+walletColumns+" FROM wallets WHERE player_id = $1 AND currency = $2",
		playerID, string(currency),
	)
	w, err := scanWallet(row)
	if err != nil {
		return nil, mapErr(err, "wallet.GetByPlayerAndCurrency")
	}
	return w, nil
}

func (r *WalletRepository) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := q(ctx, r.pool).Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().MinorUnits(), w.Version(), w.CreatedAt(), w.UpdatedAt(),
	)
	if err != nil {
		return mapErr(err, "wallet.Insert")
	}
	return nil
}

func (r *WalletRepository) Update(ctx context.Context, w *wallet.Wallet) error {
	tag, err := q(ctx, r.pool).Exec(ctx,
		`UPDATE wallets SET balance = $2, version = $3, updated_at = $4 WHERE id = $1`,
		w.ID(), w.Balance().MinorUnits(), w.Version(), w.UpdatedAt(),
	)
	if err != nil {
		return mapErr(err, "wallet.Update")
	}
	if tag.RowsAffected() == 0 {
		return mapErr(pgx.ErrNoRows, "wallet.Update")
	}
	return nil
}
