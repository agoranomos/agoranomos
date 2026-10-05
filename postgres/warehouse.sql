-- The warehouse instance (ADR-0003). This file creates the marts schema and the read-only role that
-- Metabase reads it as; the roles that load and transform data come with the pipelines. db-init runs it
-- on every start, so each statement is safe to repeat.

-- ALTER ROLE below carries a password in its text, and pg_stat_statements keeps the text of utility
-- statements in clear, for the Ops collection to read. This session leaves them out; only a superuser
-- can change the setting, and it ends with the session.
SET pg_stat_statements.track_utility = off;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS marts;

SELECT 'CREATE ROLE metabase_reader'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_reader') \gexec
SELECT format('ALTER ROLE metabase_reader WITH LOGIN PASSWORD %L', :'metabase_reader_password') \gexec
-- Metabase reads marts only, and its sessions cannot write even where a grant would allow it.
ALTER ROLE metabase_reader SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE warehouse TO metabase_reader;
GRANT USAGE ON SCHEMA marts TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_reader;
