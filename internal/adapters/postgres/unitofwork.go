package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UnitOfWork implements ports.UnitOfWork with a real pgx transaction. Every
// repository call made through the ctx WithinTx/WithinRepeatableReadTx
// passes to fn runs against that same transaction, via txFromContext —
// there is exactly one transaction, and exactly one commit, per use-case
// invocation.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

func (u *UnitOfWork) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	return runInTx(ctx, tx, fn)
}

// WithinRepeatableReadTx implements the isolation guarantee reconciliation
// needs — see ports.UnitOfWork's doc comment. AccessMode is read-only as
// defense in depth: a repository call that tried to write here is a bug,
// and Postgres itself rejects it outright rather than silently allowing
// it.
func (u *UnitOfWork) WithinRepeatableReadTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := u.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin repeatable read transaction: %w", err)
	}
	return runInTx(ctx, tx, fn)
}

func runInTx(ctx context.Context, tx pgx.Tx, fn func(ctx context.Context) error) error {
	txCtx := context.WithValue(ctx, txKey{}, tx)

	if err := fn(txCtx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return fmt.Errorf("%w (rollback also failed: %v)", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
