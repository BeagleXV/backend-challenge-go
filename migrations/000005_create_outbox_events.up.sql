CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    locked_by TEXT,
    locked_at TIMESTAMPTZ
);

-- Supports the publisher worker's claim query:
-- SELECT ... WHERE published_at IS NULL AND next_attempt_at <= now()
-- ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED.
CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL;

COMMENT ON TABLE outbox_events IS 'Transactional outbox: a row is inserted in the same transaction as the domain change it originates from, and is only published afterwards by a separate worker. locked_by/locked_at coordinate multiple publisher instances disputing the same batch.';
