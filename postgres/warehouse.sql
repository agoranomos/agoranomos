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
\getenv backup_password BACKUP_DB_PASSWORD

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

-- ALTER ROLE keeps every attribute that it does not name, so the statements below name LOGIN and
-- each attribute that would give a role more power: one given by hand, such as CREATEDB, is gone
-- after the next start.
SELECT 'CREATE ROLE loader' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'loader') \gexec
SELECT format(
  'ALTER ROLE loader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'loader_password'
) \gexec
SELECT 'CREATE ROLE transformer'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transformer') \gexec
SELECT format(
  'ALTER ROLE transformer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'transformer_password'
) \gexec
SELECT 'CREATE ROLE metabase_reader'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_reader') \gexec
SELECT format(
  'ALTER ROLE metabase_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'metabase_reader_password'
) \gexec
GRANT CONNECT ON DATABASE warehouse TO loader, transformer, metabase_reader;

-- backup takes the backups (ADR-0020). It may read every table of every database of this instance
-- (pg_read_all_data), the hashes of the passwords of the roles in pg_authid among them, and it
-- writes nothing: no other grant, and sessions that start read-only. It connects to warehouse only.
SELECT 'CREATE ROLE backup' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'backup') \gexec
SELECT format(
  'ALTER ROLE backup WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'backup_password'
) \gexec
GRANT pg_read_all_data TO backup;
GRANT CONNECT ON DATABASE warehouse TO backup;
ALTER ROLE backup SET default_transaction_read_only = on;

-- raw_demo holds the demo data, and dlt merges into it through raw_demo_staging. loader owns both,
-- so that it can create its tables there; transformer may read raw_demo.
CREATE SCHEMA IF NOT EXISTS raw_demo AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo OWNER TO loader;
ALTER SCHEMA raw_demo_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo GRANT SELECT ON TABLES TO transformer;
-- loader waits at most a minute for a lock, so a blocked load fails, and is tried again (ADR-0004).
ALTER ROLE loader SET lock_timeout = '1min';

-- transformer owns staging and marts, the schemas that dbt builds.
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION transformer;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION transformer;
ALTER SCHEMA staging OWNER TO transformer;
ALTER SCHEMA marts OWNER TO transformer;
-- dbt renames a table that it rebuilds, which waits for its readers, and the readers that come later
-- wait behind it. transformer waits at most a minute for a lock, so a blocked run fails and the mart
-- keeps its data.
ALTER ROLE transformer SET lock_timeout = '1min';

-- Metabase reads marts only. Its sessions start read-only, against accidental writes; the barrier itself
-- is that the role has no write grant. dbt creates the tables of marts as transformer, so the
-- default privileges that give Metabase the new tables are those of transformer.
ALTER ROLE metabase_reader SET default_transaction_read_only = on;
-- A query of Metabase that runs longer than five minutes is canceled, so that a dashboard or a
-- question cannot hold the warehouse, and Metabase holds at most 20 connections: more than the 15
-- of its pool (MB_JDBC_DATA_WAREHOUSE_MAX_CONNECTION_POOL_SIZE), and few enough that it cannot take
-- the connections that the pipelines need. Both lines run on every start, so that an existing
-- role gets them, and a value changed by hand goes back.
ALTER ROLE metabase_reader SET statement_timeout = '5min';
ALTER ROLE metabase_reader CONNECTION LIMIT 20;
GRANT USAGE ON SCHEMA marts TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE transformer IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_reader;
-- An earlier version of this file gave the same as postgres: an upgraded warehouse drops it.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA marts REVOKE SELECT ON TABLES FROM metabase_reader;
