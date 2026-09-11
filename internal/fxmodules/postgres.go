package fxmodules

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/beaglexv/backend-challenge-go/internal/adapters/postgres"
	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/platform/config"
	"github.com/beaglexv/backend-challenge-go/internal/platform/pg"
)

// PostgresModule provides the connection pool (with lifecycle-managed
// connect/ping/close) and every ports.*Repository backed by it.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		asWalletRepository,
		asWagerTransactionRepository,
		asLedgerRepository,
		asInboxRepository,
		asOutboxRepository,
		asUnitOfWork,
	),
)

// newPool opens the pool and registers its lifecycle: OnStart pings it so a
// misconfigured or unreachable database fails the process at startup, not
// on the first request; OnStop closes it. This must only run after every
// component that uses the pool (repositories, use cases, and — once they
// exist — the HTTP server and workers) has itself stopped, which fx
// guarantees by invoking OnStop hooks in the reverse order their
// constructors ran.
func newPool(lc fx.Lifecycle, cfg *config.Config) (*pgxpool.Pool, error) {
	pool, err := pg.NewPool(context.Background(), pg.Config{
		DSN:      cfg.Postgres.DSN(),
		MaxConns: cfg.Postgres.MaxConns,
	})
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return pool.Ping(ctx)
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

func asWalletRepository(pool *pgxpool.Pool) ports.WalletRepository {
	return postgres.NewWalletRepository(pool)
}

func asWagerTransactionRepository(pool *pgxpool.Pool) ports.WagerTransactionRepository {
	return postgres.NewWagerTransactionRepository(pool)
}

func asLedgerRepository(pool *pgxpool.Pool) ports.LedgerRepository {
	return postgres.NewLedgerRepository(pool)
}

func asInboxRepository(pool *pgxpool.Pool) ports.InboxRepository {
	return postgres.NewInboxRepository(pool)
}

func asOutboxRepository(pool *pgxpool.Pool) ports.OutboxRepository {
	return postgres.NewOutboxRepository(pool)
}

func asUnitOfWork(pool *pgxpool.Pool) ports.UnitOfWork {
	return postgres.NewUnitOfWork(pool)
}
