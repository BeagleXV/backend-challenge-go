// Package reconciliation implements the read-only use case that
// reconstructs a wallet's balance from its ledger and compares it against
// the stored value. It never mutates anything.
package reconciliation

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
)

// Result mirrors the contract's reconciliation response shape.
type Result struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int
}

type Service struct {
	uow     ports.UnitOfWork
	wallets ports.WalletRepository
	ledgers ports.LedgerRepository
}

func New(uow ports.UnitOfWork, wallets ports.WalletRepository, ledgers ports.LedgerRepository) *Service {
	return &Service{uow: uow, wallets: wallets, ledgers: ledgers}
}

// Reconcile recomputes the wallet's balance from its ledger (including the
// OPENING entry) and compares it against the stored balance. Difference is
// storedBalance - calculatedBalance, per the contract.
//
// Both reads must observe the same consistent snapshot for the comparison
// to be meaningful under concurrent writes. UnitOfWork alone does not
// guarantee that under Postgres's default READ COMMITTED isolation — the
// concrete adapter (Fase 4/12) must run this specific use case inside a
// REPEATABLE READ (or stricter) transaction.
func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (Result, error) {
	var result Result
	err := s.uow.WithinTx(ctx, func(ctx context.Context) error {
		w, err := s.wallets.GetByID(ctx, walletID)
		if err != nil {
			return fmt.Errorf("load wallet: %w", err)
		}

		entries, err := s.ledgers.ListByWallet(ctx, walletID)
		if err != nil {
			return fmt.Errorf("list ledger entries: %w", err)
		}

		calculated, err := money.Zero(w.Currency())
		if err != nil {
			return err
		}
		for _, e := range entries {
			switch e.Direction() {
			case ledger.DirectionCredit:
				calculated, err = calculated.Add(e.Amount())
			case ledger.DirectionDebit:
				calculated, err = calculated.Sub(e.Amount())
			default:
				return fmt.Errorf("reconciliation: unknown ledger direction %q", e.Direction())
			}
			if err != nil {
				return fmt.Errorf("accumulate ledger entries: %w", err)
			}
		}

		difference, err := w.Balance().Sub(calculated)
		if err != nil {
			return fmt.Errorf("compute difference: %w", err)
		}

		result = Result{
			WalletID:          w.ID(),
			StoredBalance:     w.Balance(),
			CalculatedBalance: calculated,
			Difference:        difference,
			Consistent:        difference.IsZero(),
			CheckedEntries:    len(entries),
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}
