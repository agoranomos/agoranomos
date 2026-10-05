-- The platform instance holds the metadata database of Airflow and the application database of
-- Metabase, each owned by its own role (ADR-0003). db-init runs this file on every start, so each
-- statement is safe to repeat, and a password changed in .env reaches its role.

-- ALTER ROLE below carries a password in its text, and pg_stat_statements keeps the text of utility
-- statements in clear, for the Ops collection to read. This session leaves them out; only a superuser
-- can change the setting, and it ends with the session.
SET pg_stat_statements.track_utility = off;

-- The passwords come from the environment of db-init, never from a command line, where other users of a
-- Linux host could read them. Export the variables when you run this file by hand.
\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv metabase_password METABASE_DB_PASSWORD

-- Run again, this file would print a notice for every object that exists already.
SET client_min_messages = warning;

SELECT 'CREATE ROLE airflow' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec
SELECT format('ALTER ROLE airflow WITH LOGIN PASSWORD %L', :'airflow_password') \gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
REVOKE ALL ON DATABASE airflow FROM PUBLIC;

SELECT 'CREATE ROLE metabase' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase') \gexec
SELECT format('ALTER ROLE metabase WITH LOGIN PASSWORD %L', :'metabase_password') \gexec
SELECT 'CREATE DATABASE metabase OWNER metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase') \gexec
REVOKE ALL ON DATABASE metabase FROM PUBLIC;

-- Statement statistics for the Ops collection; its view lives in the postgres database.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
