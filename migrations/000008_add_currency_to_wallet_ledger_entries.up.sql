-- amount/balance_before/balance_after are stored as bare BIGINT minor
-- units; without a currency column here, rehydrating a ledger.Entry would
-- have to join against wallets (whose currency could, in principle, still
-- change identity across a wallet's lifetime in ways the ledger shouldn't
-- depend on). Storing the currency directly on each entry makes it a
-- self-contained, independently auditable record, consistent with how
-- wager_transactions carries its own money_currency.
ALTER TABLE wallet_ledger_entries
    ADD COLUMN currency CHAR(3) NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR'));
