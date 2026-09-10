-- wagering_runtime is a privilege group, not a login role: it carries no
-- password here (nothing secret is versioned in a migration). The actual
-- login role the application connects as is created separately during
-- environment provisioning (Docker Compose init / deployment secrets) and
-- granted membership in this role there, e.g.
--   GRANT wagering_runtime TO wagering_app_login;
-- Migrations themselves always run as the schema owner, never as this role.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'wagering_runtime') THEN
        CREATE ROLE wagering_runtime NOLOGIN;
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE ON wallets TO wagering_runtime;
GRANT SELECT, INSERT, UPDATE ON wager_transactions TO wagering_runtime;
-- No UPDATE/DELETE: append-only, enforced here again on top of the triggers.
GRANT SELECT, INSERT ON wallet_ledger_entries TO wagering_runtime;
GRANT SELECT, INSERT, UPDATE ON inbox_messages TO wagering_runtime;
GRANT SELECT, INSERT, UPDATE ON outbox_events TO wagering_runtime;
