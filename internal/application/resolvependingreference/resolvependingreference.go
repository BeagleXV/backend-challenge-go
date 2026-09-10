// Package resolvependingreference implements the use case that retries
// resolution of REFUND/ROLLBACK operations parked in PENDING_REFERENCE. It
// is a thin wrapper: all the actual resolution logic (finding the
// reference, validating it, applying the reversal, or giving up) lives in
// processwagertransaction.Service.Resume, so this path can never diverge
// from what the synchronous HTTP/SQS path decides. The backoff/TTL
// scheduling policy around repeated attempts belongs to the dedicated
// worker (Fase 10), not to this use case.
package resolvependingreference

import (
	"context"

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

// ListReady returns transactions currently in PENDING_REFERENCE, locked for
// update, ready for another resolution attempt. The worker (Fase 10) is
// responsible for filtering by next_attempt_at/backoff before calling this.
func (s *Service) ListReady(ctx context.Context, limit int) ([]*wagertransaction.WagerTransaction, error) {
	return s.txs.ListPendingReferenceForUpdate(ctx, limit)
}
