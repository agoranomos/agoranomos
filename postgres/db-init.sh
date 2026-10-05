#!/bin/sh
# Create or update the roles, databases, schemas and extensions of both PostgreSQL instances.
# The db-init service runs this script on every start; every statement in the SQL files is safe to repeat.
set -eu

run_sql() {
  host=$1 database=$2 password=$3 file=$4
  shift 4
  PGPASSWORD=$password psql --no-psqlrc --quiet --set ON_ERROR_STOP=1 \
    --host "$host" --username postgres --dbname "$database" "$@" --file "$file"
}

run_sql platform-db postgres "$PLATFORM_POSTGRES_PASSWORD" /opt/agoranomos/postgres/platform.sql \
  --set airflow_password="$AIRFLOW_DB_PASSWORD" --set metabase_password="$METABASE_DB_PASSWORD"
run_sql warehouse-db warehouse "$WAREHOUSE_POSTGRES_PASSWORD" /opt/agoranomos/postgres/warehouse.sql \
  --set metabase_reader_password="$METABASE_READER_PASSWORD"
echo "db-init: the platform and warehouse instances are up to date"
