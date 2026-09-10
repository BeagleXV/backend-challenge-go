CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    origin TEXT NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind TEXT NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),

    -- External-only metadata. NULL for INTERNAL (OPENING); required for EXTERNAL.
    provider_id TEXT,
    external_transaction_id TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    round_id TEXT,
    game_id TEXT,
    reference_external_transaction_id TEXT,
    resolved_reference_id UUID REFERENCES wager_transactions (id),

    wallet_id UUID NOT NULL REFERENCES wallets (id),
    player_id UUID NOT NULL,

    money_amount BIGINT NOT NULL CHECK (money_amount >= 0),
    money_currency CHAR(3) NOT NULL CHECK (money_currency IN ('BRL', 'USD', 'EUR')),

    failure_code TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Distinguishes internal wallet-opening from provider-originated
    -- operations at the schema level: an INTERNAL row can only be a bare
    -- OPENING with no external metadata; an EXTERNAL row is never OPENING
    -- and always carries the full set of provider/idempotency metadata.
    CONSTRAINT wager_transactions_origin_fields CHECK (
        (
            origin = 'INTERNAL'
            AND kind = 'OPENING'
            AND provider_id IS NULL
            AND external_transaction_id IS NULL
            AND idempotency_key IS NULL
            AND payload_hash IS NULL
            AND round_id IS NULL
            AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
        ) OR (
            origin = 'EXTERNAL'
            AND kind <> 'OPENING'
            AND provider_id IS NOT NULL
            AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL
            AND game_id IS NOT NULL
        )
    ),

    -- REFUND/ROLLBACK always carry a reference; no other kind does.
    CONSTRAINT wager_transactions_reference_required CHECK (
        (kind IN ('REFUND', 'ROLLBACK')) = (reference_external_transaction_id IS NOT NULL)
    ),

    -- A stable failure code is mandatory on the two failure terminal
    -- states, and absent everywhere else.
    CONSTRAINT wager_transactions_failure_code_on_terminal CHECK (
        (status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
    )
);

-- One (providerId, externalTransactionId) identifies a single external operation.
CREATE UNIQUE INDEX wager_transactions_external_unique
    ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';

-- Idempotency keys are unique across the whole system, not just per provider.
CREATE UNIQUE INDEX wager_transactions_idempotency_key_unique
    ON wager_transactions (idempotency_key)
    WHERE origin = 'EXTERNAL';

-- At most one OPENING per wallet: prevents a duplicate initial credit.
CREATE UNIQUE INDEX wager_transactions_opening_unique
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

-- A given (provider, reference) can have at most one successfully
-- processed REFUND and, independently, at most one successfully processed
-- ROLLBACK: no reference gets two successful reversals of the same type.
CREATE UNIQUE INDEX wager_transactions_single_successful_reversal
    ON wager_transactions (provider_id, reference_external_transaction_id, kind)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

-- Supports the pending-reference worker's polling query.
CREATE INDEX wager_transactions_pending_reference_idx
    ON wager_transactions (status)
    WHERE status = 'PENDING_REFERENCE';

COMMENT ON TABLE wager_transactions IS 'A single wager operation: the internal wallet OPENING, or one of the five external kinds. State machine: PENDING -> PENDING_REFERENCE|PROCESSED|REJECTED|FAILED, PENDING_REFERENCE -> PROCESSED|REJECTED|FAILED. PROCESSED/REJECTED/FAILED are terminal.';
