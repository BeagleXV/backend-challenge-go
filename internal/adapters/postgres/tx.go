// Package postgres implements every application/ports interface against
// PostgreSQL via pgx v5, with SQL written and reviewed explicitly (no
// query builder, no ORM).
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is the subset of pgx.Tx and *pgxpool.Pool every repository needs.
// Both types satisfy it structurally, so a repository can run either inside
// the current UnitOfWork's transaction or, when there is none, directly
// against the pool for a standalone read.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// txFromContext returns the transaction UnitOfWork.WithinTx placed on ctx,
// if any.
func txFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// q resolves the querier to use for this call: the active transaction when
// called from within UnitOfWork.WithinTx, or the pool directly for a
// standalone read (e.g. a plain HTTP GET handler that never opened a
// transaction).
func q(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := txFromContext(ctx); ok {
		return tx
	}
	return pool
}
