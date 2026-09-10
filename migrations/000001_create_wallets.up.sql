CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency CHAR(3) NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR')),
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

COMMENT ON TABLE wallets IS 'Financial aggregate root. balance is stored in minor units (cents) and must never go negative; version increments only on a successful balance change.';
