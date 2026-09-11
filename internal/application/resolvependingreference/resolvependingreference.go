// Package resolvependingreference implements the use case that retries
// resolution of REFUND/ROLLBACK operations parked in PENDING_REFERENCE. It
// is a thin wrapper: all the actual resolution logic (finding the
// reference, validating it, applying the reversal, scheduling the next
// retry, or forcibly giving up) lives in processwagertransaction.Service
// (Resume and ExpirePendingReference), so this path can never diverge from
// what the synchronous HTTP/SQS path decides. The pending-reference worker
// (Fase 10, internal/adapters/referenceworker) owns the actual polling
// loop and the attempts/TTL policy that decides Resolve vs Expire for a
// given candidate — this package only exposes the two operations it needs.
package resolvependingreference

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/beaglexv/backend-challenge-go/internal/application/ports"
	"github.com/beaglexv/backend-challenge-go/internal/application/processwagertransaction"
	"github.com/beaglexv/backend-challenge-go/internal/domain/wagertransaction"
)

type Service struct {
	txs       ports.WagerTransactionRepository
	processor *processwagertransaction.Service
}

func New(txs ports.WagerTransactionRepository, processor *processwagertransaction.Service) *Service {
	return &Service{txs: txs, processor: processor}
}

// Resolve attempts to resolve a single PENDING_REFERENCE transaction right
// now. Returns the same Result shape processwagertransaction.Handle does.
func (s *Service) Resolve(ctx context.Context, transactionID uuid.UUID, correlationID string) (processwagertransaction.Result, error) {
	return s.processor.Resume(ctx, transactionID, correlationID)
}

// Expire forcibly rejects a PENDING_REFERENCE transaction whose retry
// budget the caller (the worker) has determined is exhausted.
func (s *Service) Expire(ctx context.Context, transactionID uuid.UUID, correlationID string) (processwagertransaction.Result, error) {
	return s.processor.ExpirePendingReference(ctx, transactionID, correlationID)
}

// ListReady returns up to limit transactions in PENDING_REFERENCE whose
// scheduled next attempt is due at or before now.
func (s *Service) ListReady(ctx context.Context, now time.Time, limit int) ([]*wagertransaction.WagerTransaction, error) {
	return s.txs.ListPendingReferenceForUpdate(ctx, now, limit)
}
