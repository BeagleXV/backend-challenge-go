ALTER TABLE wager_transactions
    DROP CONSTRAINT IF EXISTS wager_transactions_result_balance_pair,
    DROP COLUMN IF EXISTS result_balance_amount,
    DROP COLUMN IF EXISTS result_balance_currency;
