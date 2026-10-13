#!/bin/sh
# Create or update the roles, databases, schemas and extensions of both PostgreSQL instances, and
# delete the old results of Celery.
# The db-init service runs this script on every start; every statement in the SQL files is safe to repeat.
# The passwords of the roles reach the SQL files through the environment (\getenv in the files), never
# through the arguments of psql, which other users of a Linux host can read. The password of the role
# that psql signs in as reaches it in PGPASSWORD, which is in its environment too.
set -eu

run_sql() {
  host=$1 database=$2 user=$3 password=$4 file=$5
  PGPASSWORD=$password psql --no-psqlrc --quiet --set ON_ERROR_STOP=1 \
    --host "$host" --username "$user" --dbname "$database" --file "$file"
}

run_sql platform-db postgres postgres "$PLATFORM_POSTGRES_PASSWORD" /opt/agoranomos/postgres/platform.sql
run_sql warehouse-db warehouse postgres "$WAREHOUSE_POSTGRES_PASSWORD" /opt/agoranomos/postgres/warehouse.sql
# As the role celery_results, which platform.sql has just set up, never as the superuser: the code
# of a task on the Celery worker can sign in as that role too, and so decides what runs in its
# database (celery_results.sql). The results are no setup: when this fails, they stay until the
# next start, and the start goes on.
if ! run_sql platform-db celery_results celery_results "$CELERY_RESULTS_DB_PASSWORD" \
  /opt/agoranomos/postgres/celery_results.sql; then
  echo "db-init: the old results of Celery stay until the next start: deleting them failed" >&2
fi
echo "db-init: the platform and warehouse instances are up to date"
