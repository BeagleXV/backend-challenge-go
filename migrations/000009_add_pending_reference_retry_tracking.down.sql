DROP INDEX wager_transactions_pending_reference_ready_idx;

CREATE INDEX wager_transactions_pending_reference_idx
    ON wager_transactions (status)
    WHERE status = 'PENDING_REFERENCE';

ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_pending_reference_retry_fields,
    DROP COLUMN pending_reference_next_attempt_at,
    DROP COLUMN pending_reference_attempts;
