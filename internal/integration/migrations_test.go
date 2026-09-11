//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestMigrations_UpAndDown proves the migration set is fully reversible:
// every down migration undoes its up migration cleanly, and the schema
// really is gone afterwards (not just "migrate says it ran"). This uses
// its own container — never shared with another test — so it is free to
// leave the schema in whatever state it wants.
func TestMigrations_UpAndDown(t *testing.T) {
	pg := startPostgres(t) // already applies Up() once via the shared helper
	ctx := context.Background()

	var exists bool
	require.NoError(t, pg.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'wallets')",
	).Scan(&exists))
	require.True(t, exists, "wallets table must exist after Up()")

	migrateDSN := "pgx5" + pg.dsn[len("postgres"):]
	m, err := migrate.New("file://../../migrations", migrateDSN)
	require.NoError(t, err)
	defer func() { _, _ = m.Close() }()

	require.NoError(t, m.Down())

	require.NoError(t, pg.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'wallets')",
	).Scan(&exists))
	require.False(t, exists, "wallets table must be gone after a full Down()")

	// Leave it usable in case a future test shares this pattern — proves
	// Up() is idempotent-from-empty after a full revert, not just once.
	require.NoError(t, m.Up())
	require.NoError(t, pg.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'wallets')",
	).Scan(&exists))
	require.True(t, exists)
}

// TestConstraints_DirectSQLViolationsAreRejected attempts several
// constraint violations directly against the schema, bypassing every
// repository and its own application-level validation entirely — proving
// the database itself, not just the Go code in front of it, refuses
// invalid rows. Ledger immutability and the wallet
// (player_id, currency) uniqueness are already covered by
// internal/adapters/postgres/postgres_integration_test.go; this covers
// the constraints that test file does not.
func TestConstraints_DirectSQLViolationsAreRejected(t *testing.T) {
	pg := startPostgres(t)
	ctx := context.Background()

	t.Run("wallet balance cannot be negative", func(t *testing.T) {
		_, err := pg.pool.Exec(ctx,
			`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'BRL', -100, 1)`,
			uuid.New(), uuid.New(),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "wallets_balance_check")
	})

	t.Run("wallet currency must be one of the supported set", func(t *testing.T) {
		_, err := pg.pool.Exec(ctx,
			`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'XXX', 0, 1)`,
			uuid.New(), uuid.New(),
		)
		require.Error(t, err)
	})

	t.Run("wager_transactions.kind rejects unknown values", func(t *testing.T) {
		walletID := uuid.New()
		_, err := pg.pool.Exec(ctx,
			`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'BRL', 0, 1)`,
			walletID, uuid.New(),
		)
		require.NoError(t, err)

		_, err = pg.pool.Exec(ctx, `
			INSERT INTO wager_transactions (
				id, origin, kind, status, wallet_id, player_id, money_amount, money_currency
			) VALUES ($1, 'INTERNAL', 'NOT_A_REAL_KIND', 'PENDING', $2, $3, 0, 'BRL')`,
			uuid.New(), walletID, uuid.New(),
		)
		require.Error(t, err)
	})

	t.Run("wager_transactions rejects an idempotency key reused across rows", func(t *testing.T) {
		walletID := uuid.New()
		_, err := pg.pool.Exec(ctx,
			`INSERT INTO wallets (id, player_id, currency, balance, version) VALUES ($1, $2, 'BRL', 100, 1)`,
			walletID, uuid.New(),
		)
		require.NoError(t, err)

		insertBet := func(externalTxID string) error {
			_, err := pg.pool.Exec(ctx, `
				INSERT INTO wager_transactions (
					id, origin, kind, status, provider_id, external_transaction_id,
					idempotency_key, payload_hash, round_id, game_id,
					wallet_id, player_id, money_amount, money_currency
				) VALUES (
					$1, 'EXTERNAL', 'BET', 'PENDING', 'provider-a', $2,
					'provider-a:same-key', 'hash', 'round-1', 'game-1',
					$3, $4, 100, 'BRL'
				)`,
				uuid.New(), externalTxID, walletID, uuid.New(),
			)
			return err
		}

		require.NoError(t, insertBet("bet-unique-1"))
		err = insertBet("bet-unique-2") // different external id, same idempotency_key
		require.Error(t, err)
		require.Contains(t, err.Error(), "wager_transactions_idempotency_key_unique")
	})
}
