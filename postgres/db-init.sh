#!/bin/sh
# Create or update the roles, databases, schemas and extensions of both PostgreSQL instances, apply
# the migrations of the core that the warehouse has not applied yet, and delete the old results of
# Celery.
# The db-init service runs this script on every start: every statement in the SQL files is safe to
# repeat, and each migration runs once (migrate below). ./agoranomos restore runs it before it
# restores the databases with DB_INIT_MIGRATIONS=skip, which leaves the migrations to the start
# that follows.
# The passwords of the roles reach the SQL files through the environment (\getenv in the files), never
# through the arguments of psql, which other users of a Linux host can read. The password of the role
# that psql signs in as reaches it in PGPASSWORD, which is in its environment too.
set -eu

# The migrations of the core (ADR-0038, ADR-0044): the files NNNN_name.sql, numbered from 0001.
MIGRATIONS=/opt/agoranomos/postgres/migrations
# The separator of the fields of a row that psql prints: the unit separator, a control character,
# which no number, name or checksum holds.
FIELD=$(printf '\037')

# Run an SQL file, or the SQL on standard input when the file is -, with the options of psql that
# follow it.
run_sql() {
  host=$1 database=$2 user=$3 password=$4 file=$5
  shift 5
  PGPASSWORD=$password psql --no-psqlrc --quiet --set ON_ERROR_STOP=1 \
    --host "$host" --username "$user" --dbname "$database" --file "$file" "$@"
}

fail() {
  echo "db-init: $*" >&2
  exit 1
}

# Whether this run applies the migrations of the core, from DB_INIT_MIGRATIONS in its environment:
# apply, the default, or skip, with which ./agoranomos restore runs db-init before pg_restore
# replaces the warehouse whole. The roles and schemas that pg_restore needs, migrator among them,
# come either way; the db-init of the start after the restore applies the migrations, or stops on
# a warehouse that a newer release migrated. compose.yaml does not pass the setting, so every other
# start applies them. Any other value stops db-init before it changes anything.
DB_INIT_MIGRATIONS=${DB_INIT_MIGRATIONS:-apply}
case "$DB_INIT_MIGRATIONS" in
  apply | skip) ;;
  *) fail "DB_INIT_MIGRATIONS is apply or skip, not $DB_INIT_MIGRATIONS: db-init changed nothing" ;;
esac

# Apply the migrations of the core that the warehouse lacks, in the order of their numbers, each in a
# transaction of its own with its row in core.schema_migrations (migrate.sql). The files are checked
# first: four digits, an underscore, a name of lower-case letters, digits and underscores, and .sql,
# numbered from 0001 with no gap and no duplicate. Every migration that the table lists must still
# be in the folder under the same name and with the same checksum, the SHA-256 of the bytes of the
# file: an applied migration never changes, so db-init stops and applies nothing when one did, or
# when the warehouse has one that this checkout lacks, as after a newer release. The first migration
# creates the table, so a warehouse without it has applied none. With DB_INIT_MIGRATIONS=skip it
# neither reads nor applies anything, and says so.
migrate() {
  if [ "${DB_INIT_MIGRATIONS:-apply}" = skip ]; then
    echo "db-init: left the migrations of the core to the next start (DB_INIT_MIGRATIONS=skip)"
    return 0
  fi
  count=0
  for file in "$MIGRATIONS"/*; do
    [ -e "$file" ] || continue
    base=${file##*/}
    name=${base#[0-9][0-9][0-9][0-9]_}
    name=${name%.sql}
    case "$base" in
      [0-9][0-9][0-9][0-9]_*.sql) ;;
      *) fail "postgres/migrations holds $base, which is not a migration: each is NNNN_name.sql" ;;
    esac
    case "$name" in
      '' | *[!a-z0-9_]*) fail "postgres/migrations holds $base: the name of a migration has only lower-case letters, digits and underscores" ;;
    esac
    count=$((count + 1))
    expected=$(printf '%04d' "$count")
    [ "${base%%_*}" = "$expected" ] ||
      fail "postgres/migrations holds $base where $expected comes: the migrations are numbered from 0001, with no gap and no duplicate"
  done
  applied=$(run_sql warehouse-db warehouse postgres "$WAREHOUSE_POSTGRES_PASSWORD" - \
    --tuples-only --no-align --field-separator="$FIELD" << 'EOF'
SELECT to_regclass('core.schema_migrations') IS NOT NULL AS listed \gset
\if :listed
SELECT version, name, checksum FROM core.schema_migrations ORDER BY version;
\endif
EOF
  ) || fail "could not read core.schema_migrations: the error above says why"
  done_count=0
  while IFS=$FIELD read -r version name checksum; do
    [ -n "$version" ] || continue
    done_count=$((done_count + 1))
    number=$(printf '%04d' "$version")
    [ "$version" -eq "$done_count" ] ||
      fail "the warehouse has applied the migration ${number}_$name but not $(printf '%04d' "$done_count"), which comes before it: db-init applies nothing until that is mended"
    file="$MIGRATIONS/${number}_$name.sql"
    [ -f "$file" ] ||
      fail "the warehouse has applied the migration ${number}_$name, which postgres/migrations of this checkout does not hold under that name: a newer release applied it, or the file was renamed or removed. db-init applies nothing until the checkout has it as it was"
    found=$(sha256sum < "$file")
    found=${found%% *}
    [ "$found" = "$checksum" ] ||
      fail "the migration ${number}_$name.sql changed after the warehouse applied it: it had the checksum $checksum, and the file has $found now. An applied migration never changes: put the file back as the release that brought it has it, and make the change in a new migration. db-init applies nothing until then"
  done << EOF
$applied
EOF
  if [ "$done_count" -eq "$count" ]; then
    echo "db-init: the migrations of the core are up to date: $count applied, none waiting"
    return 0
  fi
  # The files are numbered from 0001 with no gap, so the place of a file in the folder is its number.
  index=0
  for file in "$MIGRATIONS"/*.sql; do
    index=$((index + 1))
    [ "$index" -gt "$done_count" ] || continue
    base=${file##*/}
    name=${base#*_}
    checksum=$(sha256sum < "$file")
    MIGRATION_FILE=$file
    MIGRATION_VERSION=$index
    MIGRATION_NAME=${name%.sql}
    MIGRATION_CHECKSUM=${checksum%% *}
    export MIGRATION_FILE MIGRATION_VERSION MIGRATION_NAME MIGRATION_CHECKSUM
    run_sql warehouse-db warehouse postgres "$WAREHOUSE_POSTGRES_PASSWORD" /opt/agoranomos/postgres/migrate.sql ||
      fail "the migration $base failed, and nothing of it was applied: the error above says why"
    echo "db-init: applied the migration $base"
  done
}

run_sql platform-db postgres postgres "$PLATFORM_POSTGRES_PASSWORD" /opt/agoranomos/postgres/platform.sql
run_sql warehouse-db warehouse postgres "$WAREHOUSE_POSTGRES_PASSWORD" /opt/agoranomos/postgres/warehouse.sql
# After warehouse.sql, which creates the role migrator and the schema core.
migrate
# As the role celery_results, which platform.sql has just set up, never as the superuser: the code
# of a task on the Celery worker can sign in as that role too, and so decides what runs in its
# database (celery_results.sql). The results are no setup: when this fails, they stay until the
# next start, and the start goes on.
if ! run_sql platform-db celery_results celery_results "$CELERY_RESULTS_DB_PASSWORD" \
  /opt/agoranomos/postgres/celery_results.sql; then
  echo "db-init: the old results of Celery stay until the next start: deleting them failed" >&2
fi
echo "db-init: the platform and warehouse instances are up to date"
