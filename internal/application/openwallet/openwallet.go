// Package openwallet implements the wallet-opening use case: create the
// wallet and, when the initial balance is positive, the internal OPENING
// transaction, its ledger credit and the corresponding outbox events — all
// atomically. A zero initial balance creates only the wallet.
package openwallet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/domain/event"
	"github.com/beaglexv/backend-challenge-go/internal/domain/ledger"
	"github.com/beaglexv/backend-challenge-go/internal/domain/money"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wallet"
)

var (
	ErrInvalidRequest      = errors.New("openwallet: invalid request")
	ErrWalletAlreadyExists = errors.New("openwallet: wallet already exists for this player and currency")
)

// Request carries everything needed to open a wallet. WalletID is supplied
// by the caller (an HTTP handler using ports.IDGenerator ahead of time)
// rather than generated here, so the same use case can be replayed
// deterministically in tests.
type Request struct {
	WalletID       uuid.UUID
	PlayerID       uuid.UUID
	Currency       money.Currency
	InitialBalance money.Money
	CorrelationID  string
}

// Result is the wallet snapshot right after opening.
type Result struct {
	WalletID uuid.UUID
	Balance  money.Money
	Version  int64
}

type Service struct {
	uow     ports.UnitOfWork
	wallets ports.WalletRepository
	txs     ports.WagerTransactionRepository
	ledgers ports.LedgerRepository
	outbox  ports.OutboxRepository
	clock   ports.Clock
	ids     ports.IDGenerator
}

func New(
	uow ports.UnitOfWork,
	wallets ports.WalletRepository,
	txs ports.WagerTransactionRepository,
	ledgers ports.LedgerRepository,
	outbox ports.OutboxRepository,
	clock ports.Clock,
	ids ports.IDGenerator,
) *Service {
	return &Service{
		uow:     uow,
		wallets: wallets,
		txs:     txs,
		ledgers: ledgers,
		outbox:  outbox,
		clock:   clock,
		ids:     ids,
	}
}

func (s *Service) Handle(ctx context.Context, req Request) (Result, error) {
	if req.WalletID == uuid.Nil || req.PlayerID == uuid.Nil {
		return Result{}, fmt.Errorf("%w: walletID and playerID are required", ErrInvalidRequest)
	}
	if req.InitialBalance.Currency() != req.Currency {
		return Result{}, fmt.Errorf("%w: initial balance currency does not match wallet currency", ErrInvalidRequest)
	}

	var result Result
	err := s.uow.WithinTx(ctx, func(ctx context.Context) error {
		now := s.clock.Now()

		w, err := wallet.New(wallet.NewParams{
			ID:             req.WalletID,
			PlayerID:       req.PlayerID,
			Currency:       req.Currency,
			InitialBalance: req.InitialBalance,
			Now:            now,
		})
		if err != nil {
			return fmt.Errorf("construct wallet: %w", err)
		}

		if err := s.wallets.Insert(ctx, w); err != nil {
			if errors.Is(err, ports.ErrAlreadyExists) {
				return ErrWalletAlreadyExists
			}
			return fmt.Errorf("insert wallet: %w", err)
		}

		if req.InitialBalance.IsZero() {
			result = Result{WalletID: w.ID(), Balance: w.Balance(), Version: w.Version()}
			return nil
		}

		openingTx, err := wagertransaction.NewInternalOpening(wagertransaction.NewInternalOpeningParams{
			ID:       s.ids.NewID(),
			WalletID: w.ID(),
			PlayerID: w.PlayerID(),
			Amount:   req.InitialBalance,
			Now:      now,
		})
		if err != nil {
			return fmt.Errorf("construct opening transaction: %w", err)
		}
		if err := s.txs.Insert(ctx, openingTx); err != nil {
			return fmt.Errorf("insert opening transaction: %w", err)
		}

		zero, err := money.Zero(req.Currency)
		if err != nil {
			return err
		}
		entry, err := ledger.New(ledger.NewParams{
			ID:            s.ids.NewID(),
			WalletID:      w.ID(),
			TransactionID: openingTx.ID(),
			Direction:     ledger.DirectionCredit,
			Amount:        req.InitialBalance,
			BalanceBefore: zero,
			BalanceAfter:  w.Balance(),
			CreatedAt:     now,
		})
		if err != nil {
			return fmt.Errorf("build ledger entry: %w", err)
		}
		if err := s.ledgers.Append(ctx, entry); err != nil {
			return fmt.Errorf("append ledger entry: %w", err)
		}

		openingTx.SetResultBalance(w.Balance())
		if err := openingTx.MarkProcessed(now); err != nil {
			return err
		}
		if err := s.txs.Update(ctx, openingTx); err != nil {
			return fmt.Errorf("update opening transaction: %w", err)
		}

		correlationID := req.CorrelationID
		if correlationID == "" {
			correlationID = openingTx.ID().String()
		}
		if err := s.enqueueOpeningEvents(ctx, w, openingTx, correlationID, now, zero); err != nil {
			return err
		}

		result = Result{WalletID: w.ID(), Balance: w.Balance(), Version: w.Version()}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) enqueueOpeningEvents(ctx context.Context, w *wallet.Wallet, tx *wagertransaction.WagerTransaction, correlationID string, now time.Time, before money.Money) error {
	processed, err := event.NewWagerTransactionProcessed(s.ids.NewID(), tx.ID(), correlationID, "", now, event.WagerTransactionProcessedData{
		TransactionID: tx.ID(),
		WalletID:      w.ID(),
		Kind:          string(tx.Kind()),
		Amount:        tx.Amount(),
	})
	if err != nil {
		return fmt.Errorf("build WagerTransactionProcessed: %w", err)
	}
	if err := s.enqueue(ctx, processed.ID, processed.AggregateID, string(processed.EventType), processed.OccurredAt, processed); err != nil {
		return err
	}

	balanceChanged, err := event.NewWalletBalanceChanged(s.ids.NewID(), w.ID(), correlationID, "", now, event.WalletBalanceChangedData{
		WalletID:      w.ID(),
		TransactionID: tx.ID(),
		Direction:     ledger.DirectionCredit,
		Amount:        tx.Amount(),
		BalanceBefore: before,
		BalanceAfter:  w.Balance(),
		WalletVersion: w.Version(),
	})
	if err != nil {
		return fmt.Errorf("build WalletBalanceChanged: %w", err)
	}
	return s.enqueue(ctx, balanceChanged.ID, balanceChanged.AggregateID, string(balanceChanged.EventType), balanceChanged.OccurredAt, balanceChanged)
}

func (s *Service) enqueue(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, occurredAt time.Time, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	if err := s.outbox.Enqueue(ctx, eventID, aggregateID, eventType, data, occurredAt); err != nil {
		return fmt.Errorf("enqueue %s: %w", eventType, err)
	}
	return nil
}
