#!/bin/sh
# The backups of Agoranomos (ADR-0020). The backup service runs this script on the image of
# PostgreSQL, so that pg_dump and pg_restore have the version of the servers, as the user of the
# checkout (AGORANOMOS_UID), so that the backups belong to that user:
#
#   backup.sh schedule    take a scheduled backup every day at BACKUP_TIME: the command of the
#                         service; it runs backup.sh scheduled for each one
#   backup.sh now         take a backup by hand: ./agoranomos backup
#   backup.sh health      the health check of the service: is the last good backup fresh?
#   backup.sh verify <backup> [<database>...]
#                         check a backup before ./agoranomos restore uses it
#
# A backup is a folder of BACKUP_ROOT named after the time of its start in UTC, such as
# 20261011T030012Z. It holds the dump of each database (pg_dump, custom format, zstd), the global
# objects of each instance without passwords, for reference only (db-init creates the roles), an
# archive of the extensions folder, SHA256SUMS and manifest.json, which says whether the schedule
# took it or a person did. It is written as .partial-<name> and renamed when it is complete, so a
# folder with the name of a backup is a complete one. latest.json says how the last run went.
# Every file is readable by its owner only. The retention removes scheduled backups only: a backup
# by hand, such as the one before an upgrade, stays until the person deletes it.
#
# It reads the databases as the role backup, which may read every table and write none, with its
# password in PGPASSWORD (compose.yaml), never on a command line.
set -eu

BACKUP_ROOT=${BACKUP_ROOT:-/backups}
EXTENSIONS=${EXTENSIONS:-/opt/agoranomos/extensions}
STARTED_FILE=${STARTED_FILE:-/tmp/backup-started}
ZONEINFO=${ZONEINFO:-/usr/share/zoneinfo}
WAREHOUSE_HOST=${WAREHOUSE_HOST:-warehouse-db}
PLATFORM_HOST=${PLATFORM_HOST:-platform-db}
# Each database of a backup, as name:host.
DATABASES="warehouse:$WAREHOUSE_HOST airflow:$PLATFORM_HOST metabase:$PLATFORM_HOST"
# A daily backup that is up to two hours late is still fresh: 26 hours.
FRESH_SECONDS=93600
# After a run that failed, the schedule tries again an hour later.
RETRY_SECONDS=3600
# A backup by hand waits this long for one that runs, such as the scheduled one.
LOCK_WAIT_SECONDS=600

umask 077

say() { printf 'backup: %s\n' "$*"; }
fail() {
  printf 'backup: error: %s\n' "$*" >&2
  exit 1
}

# Stop on a setting of .env that the schedule or the retention cannot use.
check_settings() {
  case "${BACKUP_TIME:-03:00}" in
    [01][0-9]:[0-5][0-9] | 2[0-3]:[0-5][0-9]) ;;
    *) fail "BACKUP_TIME must be a time of day as HH:MM, from 00:00 to 23:59" ;;
  esac
  for keep in "${BACKUP_KEEP_DAILY:-7}" "${BACKUP_KEEP_WEEKLY:-4}" "${BACKUP_KEEP_MONTHLY:-3}"; do
    case "$keep" in
      '' | *[!0-9]*) fail "BACKUP_KEEP_DAILY, BACKUP_KEEP_WEEKLY and BACKUP_KEEP_MONTHLY must be whole numbers" ;;
    esac
  done
  if [ "${BACKUP_KEEP_DAILY:-7}" -lt 1 ]; then
    fail "BACKUP_KEEP_DAILY must be at least 1: the newest backup always stays"
  fi
  zone=${TZ:-UTC}
  if [ "$zone" != UTC ] && [ ! -f "$ZONEINFO/$zone" ]; then
    fail "BACKUP_TZ is not a time zone of this image, such as Europe/Berlin: $zone"
  fi
}

# Print a field of a JSON file of this script, which writes one field to a line: the value
# without its quotes. Its values hold no quote and no comma.
field() {
  [ -f "$1" ] || return 0
  sed -n "s/^  \"$2\": \"\{0,1\}\([^\",]*\)\"\{0,1\},\{0,1\}\$/\1/p" "$1"
}

# Write latest.json: how the last run went, ok or failed with its step, the last good backup and
# its time of end, and the local time of the start of the last good scheduled backup, which the
# schedule reads. A failed run keeps them from the file before it, and so does a backup by hand
# for the last field.
write_latest() {
  result=$1 failed_step=$2
  latest="$BACKUP_ROOT/latest.json"
  last_scheduled_local=$(field "$latest" last_scheduled_local)
  if [ "$result" = ok ]; then
    last_ok=$name last_ok_epoch=$finished_epoch
    if [ "$kind" = scheduled ]; then last_scheduled_local=$started_local; fi
  else
    last_ok=$(field "$latest" last_ok)
    last_ok_epoch=$(field "$latest" last_ok_epoch)
  fi
  {
    printf '{\n'
    printf '  "result": "%s",\n' "$result"
    printf '  "attempted": "%s",\n' "$started"
    printf '  "step": "%s",\n' "$failed_step"
    printf '  "kind": "%s",\n' "$kind"
    printf '  "last_ok": "%s",\n' "$last_ok"
    printf '  "last_ok_epoch": %s,\n' "${last_ok_epoch:-0}"
    printf '  "last_scheduled_local": "%s"\n' "$last_scheduled_local"
    printf '}\n'
  } > "$latest.tmp"
  mv "$latest.tmp" "$latest"
}

# Run one step of a backup. When it fails, remove the partial backup, record the step in
# latest.json, and stop: the backups that exist stay as they are.
step() {
  label=$1
  shift
  if ! "$@"; then
    rm -rf -- "$partial"
    write_latest failed "$label"
    fail "the backup $name failed at: $label"
  fi
}

write_checksums() {
  (cd "$partial" && sha256sum -- *.dump *.sql extensions.tar.gz > SHA256SUMS)
}

# Write manifest.json: who took the backup (scheduled or manual), when, the images of the
# checkout that ran the stack (AGORANOMOS_IMAGES, which ./agoranomos hands Compose), and each file
# with its size and SHA-256.
write_manifest() {
  {
    printf '{\n'
    printf '  "format": 1,\n'
    printf '  "name": "%s",\n' "$name"
    printf '  "kind": "%s",\n' "$kind"
    printf '  "started": "%s",\n' "$started"
    printf '  "finished": "%s",\n' "$finished"
    printf '  "finished_epoch": %s,\n' "$finished_epoch"
    printf '  "images": "%s",\n' "${AGORANOMOS_IMAGES:-}"
    printf '  "files": [\n'
    separator=""
    while read -r checksum file; do
      printf '%s' "$separator"
      printf '    {"name": "%s", "bytes": %s, "sha256": "%s"}' \
        "$file" "$(wc -c < "$partial/$file" | tr -d ' ')" "$checksum"
      separator=",
"
    done < "$partial/SHA256SUMS"
    printf '\n  ]\n'
    printf '}\n'
  } > "$partial/manifest.json"
}

# Remove the partial backups of runs that were stopped. Only under the lock.
remove_partials() {
  for leftover in "$BACKUP_ROOT"/.partial-*; do
    [ -e "$leftover" ] || continue
    rm -rf -- "$leftover"
  done
}

# Read the names of the scheduled backups, newest first, and print those that the retention
# removes: every one but the newest of each of the last $1 days, $2 weeks (Monday to Sunday) and
# $3 months that have a backup. The names hold the time of the start in UTC, so days, weeks and
# months are of UTC.
expired() {
  awk -v daily="$1" -v weekly="$2" -v monthly="$3" '
    # The number of the day since 1970-01-01 of a date of the Gregorian calendar.
    function day_number(y, m, d,   era, yoe, doy, doe) {
      if (m <= 2) { y -= 1; m += 12 }
      era = int(y / 400)
      yoe = y - era * 400
      doy = int((153 * (m - 3) + 2) / 5) + d - 1
      doe = yoe * 365 + int(yoe / 4) - int(yoe / 100) + doy
      return era * 146097 + doe - 719468
    }
    {
      day = substr($0, 1, 8)
      month = substr($0, 1, 6)
      # 1970-01-01 was a Thursday, so these weeks start on a Monday.
      week = int((day_number(substr($0, 1, 4) + 0, substr($0, 5, 2) + 0, substr($0, 7, 2) + 0) + 3) / 7)
      keep = 0
      if (day != last_day) { last_day = day; if (++days <= daily) keep = 1 }
      if (week != last_week) { last_week = week; if (++weeks <= weekly) keep = 1 }
      if (month != last_month) { last_month = month; if (++months <= monthly) keep = 1 }
      if (!keep) print
    }'
}

# Remove the scheduled backups that the retention no longer keeps. A backup by hand is never
# removed here. Only under the lock.
prune() {
  names=""
  for path in "$BACKUP_ROOT"/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z; do
    [ -d "$path" ] || continue
    [ "$(field "$path/manifest.json" kind)" != manual ] || continue
    names="${path##*/}
$names"
  done
  printf '%s' "$names" |
    expired "${BACKUP_KEEP_DAILY:-7}" "${BACKUP_KEEP_WEEKLY:-4}" "${BACKUP_KEEP_MONTHLY:-3}" |
    while IFS= read -r old; do
      [ -n "$old" ] || continue
      rm -rf -- "${BACKUP_ROOT:?}/$old"
      say "removed the scheduled backup $old, which the retention no longer keeps"
    done
}

# Take a backup: $1 is scheduled (the schedule, which does not wait for another backup) or manual
# (a person, who waits up to LOCK_WAIT_SECONDS for one that runs).
take_backup() {
  kind=$1
  check_settings
  if [ ! -d "$BACKUP_ROOT" ] || [ ! -w "$BACKUP_ROOT" ]; then
    fail "cannot write the backups folder $BACKUP_ROOT: it must exist and belong to the user of AGORANOMOS_UID in .env, $(id -u)"
  fi
  exec 9> "$BACKUP_ROOT/.lock"
  if [ "$kind" = manual ]; then
    if ! flock -n 9; then
      say "another backup is running: waiting for it, at most $((LOCK_WAIT_SECONDS / 60)) minutes"
      flock -w "$LOCK_WAIT_SECONDS" 9 ||
        fail "another backup still runs after $((LOCK_WAIT_SECONDS / 60)) minutes: see ./agoranomos logs backup"
    fi
  else
    flock -n 9 || fail "another backup is running: the schedule tries again in an hour"
  fi
  started_epoch=$(date +%s)
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  started_local=$(date '+%Y-%m-%d %H:%M')
  name=$(date -u +%Y%m%dT%H%M%SZ)
  partial="$BACKUP_ROOT/.partial-$name"
  remove_partials
  mkdir "$partial"
  say "taking the $kind backup $name"
  for entry in $DATABASES; do
    database=${entry%%:*} host=${entry#*:}
    step "the dump of $database" pg_dump --host="$host" --username=backup --dbname="$database" \
      --format=custom --compress=zstd --lock-wait-timeout=5min --file="$partial/$database.dump"
  done
  step "the globals of warehouse-db" pg_dumpall --host="$WAREHOUSE_HOST" --username=backup \
    --database=warehouse --globals-only --no-role-passwords --file="$partial/globals-warehouse-db.sql"
  step "the globals of platform-db" pg_dumpall --host="$PLATFORM_HOST" --username=backup \
    --database=airflow --globals-only --no-role-passwords --file="$partial/globals-platform-db.sql"
  step "the archive of the extensions folder" tar -czf "$partial/extensions.tar.gz" -C "$EXTENSIONS" .
  step "the checksums" write_checksums
  finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  finished_epoch=$(date +%s)
  step "the manifest" write_manifest
  step "the rename of the backup" mv "$partial" "$BACKUP_ROOT/$name"
  write_latest ok ""
  say "the $kind backup $name is complete: $(du -sk "$BACKUP_ROOT/$name" | cut -f1) KiB in $((finished_epoch - started_epoch)) seconds"
  prune
}

# True when the scheduled backup of today is due: the time BACKUP_TIME of today has come, and the
# last good scheduled backup started before it. With no scheduled backup yet, at once. A backup by
# hand does not count.
due() {
  last=$(field "$BACKUP_ROOT/latest.json" last_scheduled_local)
  [ -n "$last" ] || return 0
  now=$(date '+%Y-%m-%d %H:%M')
  target="${now%% *} ${BACKUP_TIME:-03:00}"
  awk -v now="$now" -v target="$target" -v last="$last" \
    'BEGIN { exit !(now >= target && last < target) }'
}

# Wait in a loop, a minute at a time, so that a stop of Docker ends the wait at once: the trap
# stops the sleep that runs in the background, and the shell exits. Each backup runs as a child
# process, so that set -e holds in it.
schedule() {
  check_settings
  date +%s > "$STARTED_FILE"
  sleeper=""
  trap 'if [ -n "$sleeper" ]; then kill "$sleeper" 2> /dev/null || true; fi; exit 0' TERM INT
  say "a backup every day at ${BACKUP_TIME:-03:00}, ${TZ:-UTC}"
  last_try=0
  while :; do
    if due && [ $(($(date +%s) - last_try)) -ge "$RETRY_SECONDS" ]; then
      last_try=$(date +%s)
      if ! /bin/sh "$0" scheduled; then
        say "the backup failed: the schedule tries again in an hour"
      fi
    fi
    sleep 60 &
    sleeper=$!
    wait "$sleeper" || true
  done
}

health() {
  now=$(date +%s)
  last=$(field "$BACKUP_ROOT/latest.json" last_ok_epoch)
  if [ -n "$last" ] && [ "$last" -gt 0 ]; then
    if [ $((now - last)) -le "$FRESH_SECONDS" ]; then
      exit 0
    fi
    say "the last good backup is $(((now - last) / 3600)) hours old"
    exit 1
  fi
  started=$(cat "$STARTED_FILE" 2> /dev/null || printf '%s' "$now")
  if [ $((now - started)) -le "$FRESH_SECONDS" ]; then
    exit 0
  fi
  say "no good backup in the $(((now - started) / 3600)) hours since the service started"
  exit 1
}

verify() {
  [ "$#" -ge 1 ] || fail "usage: backup.sh verify <backup> [<database>...]"
  backup=$1
  shift
  case "$backup" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z) ;;
    *) fail "not the name of a backup: $backup" ;;
  esac
  folder="$BACKUP_ROOT/$backup"
  if [ ! -f "$folder/manifest.json" ] || [ ! -f "$folder/SHA256SUMS" ]; then
    fail "$backup is not a complete backup: it has no manifest.json or no SHA256SUMS"
  fi
  (cd "$folder" && sha256sum --check --quiet --strict SHA256SUMS) ||
    fail "$backup is damaged: a file does not match its SHA-256 in SHA256SUMS"
  for database in "$@"; do
    case "$database" in
      warehouse | airflow | metabase) ;;
      *) fail "not a database of a backup: $database" ;;
    esac
    pg_restore --file=/dev/null "$folder/$database.dump" ||
      fail "$backup: $database.dump does not read to its end"
  done
  say "$backup is complete, and its dumps read to their end"
}

case "${1:-}" in
  schedule) schedule ;;
  now) take_backup manual ;;
  scheduled) take_backup scheduled ;;
  health) health ;;
  verify)
    shift
    verify "$@"
    ;;
  *) fail "usage: backup.sh schedule | now | health | verify <backup> [<database>...]" ;;
esac
