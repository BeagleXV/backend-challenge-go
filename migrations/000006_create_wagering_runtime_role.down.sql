REVOKE ALL ON wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_events
    FROM wagering_runtime;
DROP ROLE IF EXISTS wagering_runtime;
