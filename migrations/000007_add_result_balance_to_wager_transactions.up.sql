-- The wallet balance observed when a transaction reaches PROCESSED or
-- REJECTED — "o resultado financeiro retornado ao provedor" (README 6.3).
-- An idempotent replay must return this exact snapshot, not the wallet's
-- current balance, which may have moved since the original processing.
-- Table-level grants from 000006_create_wagering_runtime_role already cover
-- these new columns (no explicit column list was used there), so no
-- additional GRANT is needed here.
ALTER TABLE wager_transactions
    ADD COLUMN result_balance_amount BIGINT,
    ADD COLUMN result_balance_currency CHAR(3) CHECK (result_balance_currency IN ('BRL', 'USD', 'EUR')),
    ADD CONSTRAINT wager_transactions_result_balance_pair CHECK (
        (result_balance_amount IS NULL) = (result_balance_currency IS NULL)
    );
