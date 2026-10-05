-- The platform instance holds the metadata database of Airflow and the application database of
-- Metabase, each owned by its own role (ADR-0003). db-init runs this file on every start, so each
-- statement is safe to repeat, and a password changed in .env reaches its role.

-- ALTER ROLE below carries a password in its text, and pg_stat_statements keeps the text of utility
-- statements in clear, for the Ops collection to read. This session leaves them out; only a superuser
-- can change the setting, and it ends with the session.
SET pg_stat_statements.track_utility = off;

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
