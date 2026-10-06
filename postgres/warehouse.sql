-- The warehouse instance (ADR-0003). This file creates the roles and schemas of the data layers:
-- loader is for dlt, which is to load the raw_<source> schemas (ADR-0004), transformer for dbt,
-- which is to read them and build staging and marts (ADR-0005), and metabase_reader reads marts
-- only. No role but the superuser can create a schema, so this file creates every schema that the
-- pipelines are to write. db-init runs it on every start, so each statement is safe to repeat.

-- ALTER ROLE below carries a password in its text, and pg_stat_statements keeps the text of utility
-- statements in clear, for the Ops collection to read. This session leaves them out; only a superuser
-- can change the setting, and it ends with the session.
SET pg_stat_statements.track_utility = off;

-- The passwords come from the environment of db-init, never from a command line, where other users of a
-- Linux host could read them. Export the variables when you run this file by hand.
\getenv loader_password WAREHOUSE_LOADER_PASSWORD
\getenv transformer_password WAREHOUSE_TRANSFORMER_PASSWORD
\getenv metabase_reader_password METABASE_READER_PASSWORD

-- Run again, this file would print a notice for every object that exists already.
SET client_min_messages = warning;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- The extension gives PUBLIC its views and the functions behind them, which show the calls and the
-- timings of every statement. Only the superuser reads them, as the Ops collection is to do.
REVOKE SELECT ON public.pg_stat_statements, public.pg_stat_statements_info FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements(boolean), public.pg_stat_statements_info()
FROM PUBLIC;

-- PUBLIC gets no CONNECT or TEMPORARY on the databases that take connections (template0 takes
-- none): each role gets CONNECT on warehouse below, and no more.
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
REVOKE ALL ON DATABASE postgres, template1 FROM PUBLIC;

SELECT 'CREATE ROLE loader' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'loader') \gexec
SELECT format('ALTER ROLE loader WITH LOGIN PASSWORD %L', :'loader_password') \gexec
SELECT 'CREATE ROLE transformer'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transformer') \gexec
SELECT format('ALTER ROLE transformer WITH LOGIN PASSWORD %L', :'transformer_password') \gexec
SELECT 'CREATE ROLE metabase_reader'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_reader') \gexec
SELECT format('ALTER ROLE metabase_reader WITH LOGIN PASSWORD %L', :'metabase_reader_password') \gexec
GRANT CONNECT ON DATABASE warehouse TO loader, transformer, metabase_reader;

-- raw_demo is for the demo data, and dlt is to merge into it through raw_demo_staging. loader owns
-- both, so that it can create its tables there; transformer may read raw_demo.
CREATE SCHEMA IF NOT EXISTS raw_demo AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo OWNER TO loader;
ALTER SCHEMA raw_demo_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo GRANT SELECT ON TABLES TO transformer;

-- transformer owns staging and marts, the schemas that dbt is to build.
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION transformer;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION transformer;
ALTER SCHEMA staging OWNER TO transformer;
ALTER SCHEMA marts OWNER TO transformer;

-- Metabase reads marts only. Its sessions start read-only, against accidental writes; the barrier itself
-- is that the role has no write grant. dbt is to create the tables of marts as transformer, so the
-- default privileges that give Metabase the new tables are those of transformer.
ALTER ROLE metabase_reader SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA marts TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE transformer IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_reader;
-- An earlier version of this file gave the same as postgres: an upgraded warehouse drops it.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA marts REVOKE SELECT ON TABLES FROM metabase_reader;
