CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets (id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions (id),
    direction TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wallet_ledger_entries_wallet_tx_unique UNIQUE (wallet_id, transaction_id),

    -- balanceAfter = balanceBefore +/- amount, enforced by the schema
    -- itself, independently of the domain-layer check in ledger.New.
    CONSTRAINT wallet_ledger_entries_balance_consistent CHECK (
        (direction = 'DEBIT' AND balance_after = balance_before - amount)
        OR (direction = 'CREDIT' AND balance_after = balance_before + amount)
    )
);

-- Append-only: any UPDATE or DELETE attempt is rejected outright, in
-- addition to the application never issuing one. Defense in depth against
-- both bugs and a compromised/misconfigured connection.
CREATE FUNCTION wallet_ledger_entries_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: % is not allowed', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_entries_no_update
    BEFORE UPDATE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_reject_mutation();

CREATE TRIGGER wallet_ledger_entries_no_delete
    BEFORE DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_reject_mutation();

COMMENT ON TABLE wallet_ledger_entries IS 'Immutable, append-only record of a single balance movement. LOSS and rejected operations never produce a row here.';
