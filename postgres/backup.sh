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
#                         check a backup before ./agoranomos restore uses it: its files against
#                         SHA256SUMS, and each named dump read to its end; it holds a shared lock,
#                         so that no retention removes the backup meanwhile
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
warn() { printf 'backup: warning: %s\n' "$*" >&2; }

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

# Run one step of a backup. set -e is in effect in it, in the functions too (it would not be in an
# if or an ||), so the first command that fails stops the shell, and abort, the trap on EXIT, says
# which step it was.
step() {
  current_step=$1
  shift
  "$@"
}

# When a step has failed, remove the partial backup, record the step in latest.json, and stop with
# one line: the backups that exist stay as they are.
abort() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ -n "$current_step" ]; then
    rm -rf -- "$partial" || true
    write_latest failed "$current_step" || true
    printf 'backup: error: the backup %s failed at: %s\n' "$name" "$current_step" >&2
    exit 1
  fi
  exit "$status"
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
      size=$(wc -c < "$partial/$file")
      printf '%s' "$separator"
      printf '    {"name": "%s", "bytes": %s, "sha256": "%s"}' "$file" "$((size))" "$checksum"
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

# True when no backup, and nothing else, has the name that this backup takes.
name_is_free() { [ ! -e "$BACKUP_ROOT/$name" ]; }

# Read the names of the scheduled backups, newest first, and print those that the retention
# removes: every one but the newest of each of the last $1 days, $2 weeks (Monday to Sunday) and
# $3 months that have a backup. The names hold the time of the start in UTC, so days, weeks and
# months are of UTC, while the schedule keeps the days of BACKUP_TZ. With a BACKUP_TIME near
# midnight UTC, the change to summer time can put two scheduled backups on one UTC day, and the
# older goes: once a year the restore points are two days apart.
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

# Remove the scheduled backups that the retention no longer keeps. Only a folder whose manifest
# says scheduled can go: a backup by hand, and a folder whose manifest is missing or cannot be
# read, stay until a person deletes them. Only under the lock.
prune() {
  names=""
  for path in "$BACKUP_ROOT"/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z; do
    [ -d "$path" ] || continue
    [ "$(field "$path/manifest.json" kind)" = scheduled ] || continue
    names="${path##*/}
$names"
  done
  printf '%s' "$names" |
    expired "${BACKUP_KEEP_DAILY:-7}" "${BACKUP_KEEP_WEEKLY:-4}" "${BACKUP_KEEP_MONTHLY:-3}" |
    while IFS= read -r old; do
      [ -n "$old" ] || continue
      if rm -rf -- "${BACKUP_ROOT:?}/$old"; then
        say "removed the scheduled backup $old, which the retention no longer keeps"
      else
        warn "could not remove the scheduled backup $old, which the retention no longer keeps"
      fi
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
  # command exec: a redirection of a bare exec that fails stops a POSIX shell with a message of its
  # own (and exit 2 in dash). The group keeps that message off, as fail says it in one line.
  if ! { command exec 9> "$BACKUP_ROOT/.lock"; } 2> /dev/null; then
    fail "cannot open the lock file $BACKUP_ROOT/.lock: it must belong to the user of AGORANOMOS_UID in .env, $(id -u)"
  fi
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
  current_step=""
  trap abort EXIT
  step "the removal of the partial backups of earlier runs" remove_partials
  step "the check that no backup has this name" name_is_free
  step "the folder of the backup" mkdir "$partial"
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
  step "the record in latest.json" write_latest ok ""
  current_step=""
  say "the $kind backup $name is complete: $(du -sk "$BACKUP_ROOT/$name" | cut -f1) KiB in $((finished_epoch - started_epoch)) seconds"
  # The backup is good whatever the retention does: its errors are warnings.
  prune || warn "the retention could not finish; the backup just taken is complete"
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

# Print the epoch of a time as latest.json says it, such as 2026-10-11T23:30:00Z (UTC); 0 for
# anything else. The date of the image reads no such time (date -d is GNU only), so awk counts.
epoch_of() {
  case "$1" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z) ;;
    *)
      echo 0
      return 0
      ;;
  esac
  awk -v time="$1" '
    # The number of the day since 1970-01-01 of a date of the Gregorian calendar.
    function day_number(y, m, d,   era, yoe, doy, doe) {
      if (m <= 2) { y -= 1; m += 12 }
      era = int(y / 400)
      yoe = y - era * 400
      doy = int((153 * (m - 3) + 2) / 5) + d - 1
      doe = yoe * 365 + int(yoe / 4) - int(yoe / 100) + doy
      return era * 146097 + doe - 719468
    }
    BEGIN {
      days = day_number(substr(time, 1, 4) + 0, substr(time, 6, 2) + 0, substr(time, 9, 2) + 0)
      printf "%.0f\n", days * 86400 + substr(time, 12, 2) * 3600 + substr(time, 15, 2) * 60 + substr(time, 18, 2)
    }'
}

# Wait in a loop, a minute at a time. A stop (TERM or INT) ends a sleep at once: the trap kills it
# and the shell exits. During a backup it is held until the backup ends, since the shell waits for
# its child and runs the trap afterwards; Docker kills the container when its grace period (10
# seconds by default) is over, which leaves a .partial-* folder that the next run removes, and
# latest.json as it was. Each backup runs as a child process, so that set -e holds in it.
#
# A run that failed leaves the schedule pending: the next try comes an hour after the failure,
# also after midnight, until a scheduled backup succeeds. Without it a BACKUP_TIME of the evening
# would wait for the same time of the next day. A start of the service finds it in latest.json: the
# last run was a scheduled one and it failed (a run by hand, good or not, is not that). The day's
# own rule, due, stays: one backup a day at BACKUP_TIME, and at once when there is none.
schedule() {
  check_settings
  date +%s > "$STARTED_FILE"
  sleeper=""
  trap 'if [ -n "$sleeper" ]; then kill "$sleeper" 2> /dev/null || true; fi; exit 0' TERM INT
  say "a backup every day at ${BACKUP_TIME:-03:00}, ${TZ:-UTC}"
  last_try=0
  pending=""
  recorded="$BACKUP_ROOT/latest.json"
  if [ "$(field "$recorded" result)" = failed ] && [ "$(field "$recorded" kind)" = scheduled ]; then
    pending=1
    last_try=$(epoch_of "$(field "$recorded" attempted)")
    say "the last scheduled backup failed at $(field "$recorded" attempted): the next try is an hour after it, or now"
  fi
  while :; do
    if { [ -n "$pending" ] || due; } && [ $(($(date +%s) - last_try)) -ge "$RETRY_SECONDS" ]; then
      last_try=$(date +%s)
      if /bin/sh "$0" scheduled; then
        pending=""
      else
        pending=1
        say "the backup failed: the schedule tries again in an hour"
      fi
    fi
    sleep 60 &
    sleeper=$!
    wait "$sleeper" || true
  done
}

# The service is unhealthy when the last good backup, scheduled or by hand, is more than 26 hours
# old, or when there is none 26 hours after the service started. The start counts from the file in
# /tmp, which a restart of the service renews: a service that restarts more often than every 26
# hours and never succeeds stays healthy.
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
  # A shared lock, so that the retention of a backup that runs cannot remove this one while it is
  # checked. The lock file is opened to read: the folder may be a read-only mount. With no lock file
  # no backup was ever taken, and none can run.
  lock="$BACKUP_ROOT/.lock"
  if [ -e "$lock" ]; then
    if ! { command exec 9< "$lock"; } 2> /dev/null; then
      fail "cannot read the lock file $lock: it must belong to the user of AGORANOMOS_UID in .env, $(id -u)"
    fi
    if ! flock -s -n 9; then
      say "a backup is running: waiting for it, at most $((LOCK_WAIT_SECONDS / 60)) minutes"
      flock -s -w "$LOCK_WAIT_SECONDS" 9 ||
        fail "a backup still runs after $((LOCK_WAIT_SECONDS / 60)) minutes: see ./agoranomos logs backup"
    fi
  fi
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
  if [ "$#" -eq 0 ]; then
    say "$backup: checked the files only, against SHA256SUMS; no dump was read"
  else
    say "$backup is complete, and its dumps read to their end"
  fi
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
