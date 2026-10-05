#!/bin/sh
# Create or update the roles, databases, schemas and extensions of both PostgreSQL instances.
# The db-init service runs this script on every start; every statement in the SQL files is safe to repeat.
# The passwords of the roles reach the SQL files through the environment (\getenv in the files), never
# through the arguments of psql, which other users of a Linux host can read. The password of the
# superuser reaches psql in PGPASSWORD, which is in its environment too.
set -eu

run_sql() {
  host=$1 database=$2 password=$3 file=$4
  PGPASSWORD=$password psql --no-psqlrc --quiet --set ON_ERROR_STOP=1 \
    --host "$host" --username postgres --dbname "$database" --file "$file"
}

run_sql platform-db postgres "$PLATFORM_POSTGRES_PASSWORD" /opt/agoranomos/postgres/platform.sql
run_sql warehouse-db warehouse "$WAREHOUSE_POSTGRES_PASSWORD" /opt/agoranomos/postgres/warehouse.sql
echo "db-init: the platform and warehouse instances are up to date"
