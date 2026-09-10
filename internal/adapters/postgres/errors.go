package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
)

// ErrConstraintViolation wraps a Postgres constraint failure the schema
// enforces (CHECK, NOT NULL, FK) that isn't one of the two conditions use
// cases branch on directly (not found / already exists). Reaching it
// typically means either a real bug or a genuine race the application-level
// check didn't anticipate — never a raw provider input the caller controls.
var ErrConstraintViolation = errors.New("postgres: constraint violation")

// mapErr translates a raw pgx/pgconn error into one of the stable sentinels
// application use cases branch on (ports.ErrNotFound, ports.ErrAlreadyExists)
// or ErrConstraintViolation. No *pgconn.PgError or pgx.ErrNoRows — nor the
// raw driver error string, which can embed row values via DETAIL — is ever
// allowed to escape this package; op names the failing operation for
// context, never row data.
func mapErr(err error, op string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, ports.ErrNotFound)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%s: %w (constraint %s)", op, ports.ErrAlreadyExists, pgErr.ConstraintName)
		case "23514", "23502", "23503": // check_violation, not_null_violation, foreign_key_violation
			return fmt.Errorf("%s: %w (constraint %s)", op, ErrConstraintViolation, pgErr.ConstraintName)
		}
	}

	return fmt.Errorf("%s: %w", op, err)
}
