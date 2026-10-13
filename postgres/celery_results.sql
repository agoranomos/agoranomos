-- The results of the tasks of the CeleryExecutor variant (ADR-0015) that are older than 90 days go
-- on every start: nothing in Airflow removes them from a database of their own (airflow db clean
-- knows their tables only in the metadata database). The worker creates the tables when it first
-- starts, so before that there are none.
--
-- db-init runs this file as the role celery_results, which owns the database, its schema public
-- and the tables, and never as the superuser. Whoever holds the password of the role, as the code
-- of a task on the Celery worker does, decides what a statement in this database runs, such as a
-- trigger on a table, or an operator of a type of its own: run by the superuser, that code would
-- have the powers of the superuser. The same code could make a statement wait without end, so
-- each statement here stops after a minute, and the start goes on (db-init.sh).
SET statement_timeout = '1min';
SELECT 'DELETE FROM celery_taskmeta WHERE date_done < now() - interval ''90 days'''
WHERE to_regclass('public.celery_taskmeta') IS NOT NULL \gexec
SELECT 'DELETE FROM celery_tasksetmeta WHERE date_done < now() - interval ''90 days'''
WHERE to_regclass('public.celery_tasksetmeta') IS NOT NULL \gexec
