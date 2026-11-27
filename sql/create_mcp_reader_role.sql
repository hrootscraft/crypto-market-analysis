-- Least-privilege login for the MCP server (mcp_server/).
--
-- The AI assistant only ever connects as mcp_reader, which can SELECT from the
-- dbt models in `analytics` and nothing else: no raw schema, no writes, and
-- every session defaults to read-only with a statement timeout.
--
-- Runs automatically on a fresh warehouse volume (mounted in docker-compose.yml).
-- For an existing warehouse, run it once:
--   docker compose exec -T warehouse psql -U crypto_user -d crypto < sql/create_mcp_reader_role.sql
-- Safe to re-run. Change the password for anything beyond local use.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mcp_reader') THEN
        CREATE ROLE mcp_reader LOGIN PASSWORD 'mcp_reader_pass';
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO mcp_reader', current_database());
END
$$;

CREATE SCHEMA IF NOT EXISTS analytics;

GRANT USAGE ON SCHEMA analytics TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO mcp_reader;

-- dbt drops and recreates mart tables on every run; default privileges make
-- sure each new table/view created by crypto_user is readable by mcp_reader.
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_user IN SCHEMA analytics
    GRANT SELECT ON TABLES TO mcp_reader;

REVOKE ALL ON SCHEMA raw FROM mcp_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM mcp_reader;

ALTER ROLE mcp_reader SET default_transaction_read_only = on;
ALTER ROLE mcp_reader SET statement_timeout = '10s';
ALTER ROLE mcp_reader CONNECTION LIMIT 5;
