-- The platform instance holds the metadata database of Airflow, the application database of
-- Metabase and the database of the results of Celery, each owned by its own role (ADR-0003).
-- db-init runs this file on every start, so each statement is safe to repeat, and a password
-- changed in .env reaches its role.

-- ALTER ROLE below carries a password in its text, and pg_stat_statements keeps the text of utility
-- statements in clear, for the Ops collection to read. This session leaves them out; only a superuser
-- can change the setting, and it ends with the session.
SET pg_stat_statements.track_utility = off;

-- The passwords come from the environment of db-init, never from a command line, where other users of a
-- Linux host could read them. Export the variables when you run this file by hand.
\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv metabase_password METABASE_DB_PASSWORD
\getenv backup_password BACKUP_DB_PASSWORD
\getenv celery_results_password CELERY_RESULTS_DB_PASSWORD

-- Run again, this file would print a notice for every object that exists already.
SET client_min_messages = warning;

-- ALTER ROLE keeps every attribute that it does not name, so the statements below name LOGIN and
-- each attribute that would give a role more power: one given by hand, such as CREATEDB, is gone
-- after the next start.
SELECT 'CREATE ROLE airflow' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec
SELECT format(
  'ALTER ROLE airflow WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'airflow_password'
) \gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
REVOKE ALL ON DATABASE airflow FROM PUBLIC;

SELECT 'CREATE ROLE metabase' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase') \gexec
SELECT format(
  'ALTER ROLE metabase WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'metabase_password'
) \gexec
SELECT 'CREATE DATABASE metabase OWNER metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase') \gexec
REVOKE ALL ON DATABASE metabase FROM PUBLIC;

-- celery_results holds the results of the tasks of the CeleryExecutor variant (ADR-0015): the
-- worker writes the state of each task there, and the scheduler reads it. A database and a role of
-- their own, so that the worker holds no password of the metadata database of Airflow. db-init
-- sets them up in both variants; only the Celery variant uses them. The limit of connections
-- keeps the code of a task, which can read the password, from taking those of the instance from
-- Airflow and Metabase. Such code decides what runs in the database of the role, so no statement
-- of this file, which the superuser runs, enters it: db-init deletes the old results as the role
-- itself (celery_results.sql).
SELECT 'CREATE ROLE celery_results'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'celery_results') \gexec
SELECT format(
  'ALTER ROLE celery_results WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'celery_results_password'
) \gexec
ALTER ROLE celery_results CONNECTION LIMIT 20;
SELECT 'CREATE DATABASE celery_results OWNER celery_results'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'celery_results') \gexec
REVOKE ALL ON DATABASE celery_results FROM PUBLIC;

-- backup takes the backups (ADR-0020): it may read every table of airflow and metabase
-- (pg_read_all_data), the hashes of the passwords of the roles in pg_authid among them, and it
-- writes nothing. It connects to these two databases only.
SELECT 'CREATE ROLE backup' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'backup') \gexec
SELECT format(
  'ALTER ROLE backup WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
  :'backup_password'
) \gexec
GRANT pg_read_all_data TO backup;
GRANT CONNECT ON DATABASE airflow, metabase TO backup;
ALTER ROLE backup SET default_transaction_read_only = on;

-- PUBLIC gets no CONNECT or TEMPORARY on the other databases that take connections (template0
-- takes none): each role connects to the database that it owns, and to no other.
REVOKE ALL ON DATABASE postgres, template1 FROM PUBLIC;

-- Statement statistics for the Ops collection; its view lives in the postgres database. The
-- extension gives PUBLIC its views and the functions behind them, which show the calls and the
-- timings of every statement. Only the superuser reads them, as the Ops collection is to do.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
REVOKE SELECT ON public.pg_stat_statements, public.pg_stat_statements_info FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements(boolean), public.pg_stat_statements_info()
FROM PUBLIC;
