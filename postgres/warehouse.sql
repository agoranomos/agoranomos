-- The warehouse instance (ADR-0003). This file creates the roles and schemas of the data layers:
-- loader is for dlt, which is to load the raw_<source> schemas (ADR-0004), transformer for dbt,
-- which is to read them and build staging, marts and ops (ADR-0005, ADR-0029), metabase_reader
-- reads marts and the views of the Ops collection in ops, and ai_reader, the role of MCP Toolbox,
-- reads marts and staging (ADR-0007, ADR-0030). No role but the superuser can create a
-- schema, so this file creates every schema that the pipelines are to write. db-init runs it on every start, so each statement is safe to repeat.

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
\getenv ai_reader_password AI_READER_PASSWORD
\getenv warehouse_mcp WAREHOUSE_MCP

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
-- No session of the warehouse compiles a query with JIT. The models of dbt are views over views,
-- and the code that JIT compiled for one of their tests took more memory than the test itself:
-- four of them at a time took warehouse-db past its memory limit
-- (docs/reference/system-requirements.md). The line runs on every start, for new sessions.
ALTER DATABASE warehouse SET jit = off;

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
-- ai_reader signs in only while the MCP server of the warehouse is on (WAREHOUSE_MCP in .env):
-- off, the role keeps its grants and cannot sign in, so the switch and the role are two limits.
SELECT 'CREATE ROLE ai_reader' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ai_reader') \gexec
SELECT format(
  'ALTER ROLE ai_reader WITH %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  CASE WHEN :'warehouse_mcp' = 'true' THEN 'LOGIN' ELSE 'NOLOGIN' END,
  :'ai_reader_password'
) \gexec
GRANT CONNECT ON DATABASE warehouse TO loader, transformer, metabase_reader, ai_reader;

-- backup takes the backups (ADR-0020). On this instance it connects to warehouse only, and it may
-- read every table there (pg_read_all_data), the hashes of the passwords of the roles in pg_authid
-- among them. It writes nothing: no other grant, and sessions that start read-only. The members of
-- pg_read_all_data may select from the views of pg_stat_statements, but the functions behind them
-- are revoked from PUBLIC above and not given to backup, so it reads no statistics of the statements.
SELECT 'CREATE ROLE backup' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'backup') \gexec
SELECT format(
  'ALTER ROLE backup WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'backup_password'
) \gexec
GRANT pg_read_all_data TO backup;
GRANT CONNECT ON DATABASE warehouse TO backup;
ALTER ROLE backup SET default_transaction_read_only = on;

-- The fictional platforms of the demo (ADR-0025), which replace the one demo account of v0.1: dlt
-- loads each into raw_demo_<platform>, and merges and keeps the versions of its campaigns through
-- raw_demo_<platform>_staging. loader owns both; transformer may read the first.
CREATE SCHEMA IF NOT EXISTS raw_demo_search AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_search_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo_search OWNER TO loader;
ALTER SCHEMA raw_demo_search_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo_search TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo_search TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo_search GRANT SELECT ON TABLES TO transformer;
CREATE SCHEMA IF NOT EXISTS raw_demo_social AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_social_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo_social OWNER TO loader;
ALTER SCHEMA raw_demo_social_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo_social TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo_social TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo_social GRANT SELECT ON TABLES TO transformer;
CREATE SCHEMA IF NOT EXISTS raw_demo_video AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_video_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo_video OWNER TO loader;
ALTER SCHEMA raw_demo_video_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo_video TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo_video TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo_video GRANT SELECT ON TABLES TO transformer;
CREATE SCHEMA IF NOT EXISTS raw_demo_web AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_web_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo_web OWNER TO loader;
ALTER SCHEMA raw_demo_web_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo_web TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo_web TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo_web GRANT SELECT ON TABLES TO transformer;
CREATE SCHEMA IF NOT EXISTS raw_demo_fx AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_demo_fx_staging AUTHORIZATION loader;
ALTER SCHEMA raw_demo_fx OWNER TO loader;
ALTER SCHEMA raw_demo_fx_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_demo_fx TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_demo_fx TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_demo_fx GRANT SELECT ON TABLES TO transformer;
-- The reference rates of the euro of the European Central Bank (ADR-0032), which dlt loads into
-- raw_ecb and merges through raw_ecb_staging, as it loads a platform. loader owns both;
-- transformer may read the first.
CREATE SCHEMA IF NOT EXISTS raw_ecb AUTHORIZATION loader;
CREATE SCHEMA IF NOT EXISTS raw_ecb_staging AUTHORIZATION loader;
ALTER SCHEMA raw_ecb OWNER TO loader;
ALTER SCHEMA raw_ecb_staging OWNER TO loader;
GRANT USAGE ON SCHEMA raw_ecb TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_ecb TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_ecb GRANT SELECT ON TABLES TO transformer;
-- The settings of the model of the deployment (ADR-0024): the first task of the transform loads
-- them with dlt into raw_settings, replacing each table in place, so the schema needs no staging
-- schema. loader owns it; transformer may read it, and metabase_reader may not.
CREATE SCHEMA IF NOT EXISTS raw_settings AUTHORIZATION loader;
ALTER SCHEMA raw_settings OWNER TO loader;
GRANT USAGE ON SCHEMA raw_settings TO transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_settings TO transformer;
ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_settings GRANT SELECT ON TABLES TO transformer;
-- loader waits at most a minute for a lock, so a blocked load fails, and is tried again (ADR-0004).
ALTER ROLE loader SET lock_timeout = '1min';

-- transformer owns staging and marts, the schemas that dbt builds, and ops, where dbt builds the
-- tables of elementary and the views of the Ops collection (ADR-0029).
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION transformer;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION transformer;
CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION transformer;
ALTER SCHEMA staging OWNER TO transformer;
ALTER SCHEMA marts OWNER TO transformer;
ALTER SCHEMA ops OWNER TO transformer;
-- elementary writes into its tables through temporary tables on PostgreSQL, as dbt does for an
-- incremental model (ADR-0029). A temporary table lives in the session that made it and goes with it.
GRANT TEMPORARY ON DATABASE warehouse TO transformer;
-- dbt renames a table that it rebuilds, which waits for its readers, and the readers that come later
-- wait behind it. transformer waits at most a minute for a lock, so a blocked run fails and the mart
-- keeps its data.
ALTER ROLE transformer SET lock_timeout = '1min';

-- The demo of v0.1: one fictional ad account in raw_demo, which dlt merged through
-- raw_demo_staging, the view staging.stg_demo__ad_stats_daily and the mart marts.ad_spend_daily.
-- The model of v0.2 replaces them (ADR-0025), so a warehouse of v0.1 loses them on its first
-- start of v0.2, and these statements find nothing on every later start. CASCADE drops what a
-- person built on them, as each rebuild of dbt did in v0.1. DROP TABLE and DROP VIEW fail on a
-- relation of the other kind, IF EXISTS or not, which would stop db-init on every start: so each
-- runs only on the kind that v0.1 made, and a view of the name of the mart that a person made
-- since, for the questions of v0.1, stays.
SELECT 'DROP TABLE marts.ad_spend_daily CASCADE'
WHERE EXISTS (SELECT FROM pg_tables WHERE schemaname = 'marts' AND tablename = 'ad_spend_daily') \gexec
SELECT 'DROP VIEW staging.stg_demo__ad_stats_daily CASCADE'
WHERE EXISTS (
  SELECT FROM pg_views WHERE schemaname = 'staging' AND viewname = 'stg_demo__ad_stats_daily'
) \gexec
DROP SCHEMA IF EXISTS raw_demo_staging CASCADE;
DROP SCHEMA IF EXISTS raw_demo CASCADE;

-- Metabase reads marts, and in ops the views of the Ops collection. Its sessions start read-only,
-- against accidental writes; the barrier itself is that the role has no write grant. dbt creates the tables of marts as transformer, so the
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
-- In ops, Metabase reads the views of the Ops collection only: dbt grants SELECT on each of them as
-- it builds them (the grants of models/ops in dbt_project.yml), and on no table of elementary.
GRANT USAGE ON SCHEMA ops TO metabase_reader;

-- MCP Toolbox reads marts and staging as ai_reader, and neither the raw schemas nor ops (ADR-0007,
-- ADR-0030): the views of staging read the raw schemas as their owner, transformer, so the role
-- sees the data that they show and no more. Its sessions start read-only, a statement that runs
-- longer than a minute is canceled, so is a transaction left open for a minute, and it holds at
-- most three connections: an AI client sends one statement at a time, and the pipelines keep
-- theirs. As for metabase_reader, the barrier itself is that the role has no write grant, and
-- these lines run on every start.
ALTER ROLE ai_reader SET default_transaction_read_only = on;
ALTER ROLE ai_reader SET statement_timeout = '1min';
ALTER ROLE ai_reader SET idle_in_transaction_session_timeout = '1min';
ALTER ROLE ai_reader CONNECTION LIMIT 3;
GRANT USAGE ON SCHEMA marts, staging TO ai_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO ai_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO ai_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE transformer IN SCHEMA marts GRANT SELECT ON TABLES TO ai_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE transformer IN SCHEMA staging GRANT SELECT ON TABLES TO ai_reader;
