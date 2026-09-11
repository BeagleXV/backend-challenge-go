-- Retry bookkeeping for the pending-reference worker (Fase 10):
-- pending_reference_attempts counts resolution attempts made so far (the
-- first entry into PENDING_REFERENCE counts as attempt 1) and is kept as a
-- historical record even after the transaction reaches a terminal state.
-- pending_reference_next_attempt_at is when the worker should next try —
-- non-NULL exactly while status = 'PENDING_REFERENCE', enforced below,
-- mirroring the domain layer's own invariant (WagerTransaction.transitionTo
-- clears it on every transition out of PENDING_REFERENCE).
ALTER TABLE wager_transactions
    ADD COLUMN pending_reference_attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN pending_reference_next_attempt_at TIMESTAMPTZ,
    ADD CONSTRAINT wager_transactions_pending_reference_retry_fields CHECK (
        (status = 'PENDING_REFERENCE') = (pending_reference_next_attempt_at IS NOT NULL)
    );

-- Replaces wager_transactions_pending_reference_idx (status-only): the
-- worker's polling query filters and orders by next_attempt_at, so the
-- index needs to cover that column, not just the status it's already
-- implied by the partial WHERE clause.
DROP INDEX wager_transactions_pending_reference_idx;

CREATE INDEX wager_transactions_pending_reference_ready_idx
    ON wager_transactions (pending_reference_next_attempt_at)
    WHERE status = 'PENDING_REFERENCE';
