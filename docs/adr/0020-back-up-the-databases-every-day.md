---
status: accepted
date: 2026-10-09
decision-makers: Andrei Gromov
---

# Back up the databases every day with pg_dump, into a folder of the host

## Context and Problem Statement

The state of a deployment is in three databases: the warehouse, the metadata database of Airflow and the application database of Metabase ([ADR-0003](0003-two-postgres-instances.md)), and in its extensions folder ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)). The keys that decrypt the connections that Airflow and Metabase save are in `.env` ([ADR-0014](0014-start-the-stack-with-one-script.md)). An upgrade of Metabase to a new major version migrates its database one way, so the way back is a backup made before. Server mode needs backups on a schedule ([ADR-0011](0011-local-and-server-modes.md)), and every mode needs one before an upgrade.

Facts that shape the design, as of October 2026: the PostgreSQL image of the stack has neither cron nor busybox. `pg_restore --list` passes a dump that was cut short after its table of contents, and a restore with `--clean --create --exit-on-error` that stops halfway leaves a database with its tables and without their rows. On Linux a bind mount keeps the owner of the host, so a container must write as the user who owns the folder. A role in `pg_read_all_data` reads every table, also the hashes of the passwords of the roles in `pg_authid`, and no table with row security unless it has `BYPASSRLS`; no table of the stack has row security.

How does the stack take backups, where does it keep them, and how does it restore them?

## Decision Drivers

* One command: no step on the host besides `./agoranomos`.
* Least privilege: the job that dumps can neither write nor change a database.
* A backup is checked before a restore drops anything.
* A person sees the backups as files of their own and can copy them to another machine.
* No key goes into a backup.
* Backups of the size of the warehouses of small businesses, a few gigabytes.

## Considered Options

* A long-running service on the PostgreSQL image of the stack, waiting in a loop of the shell, that dumps as a role of its own into a folder of the host
* The same service with supercronic, in an image of our own
* The same service, dumping as the superusers of the instances
* The same service, writing into a named volume of Docker
* A timer of the host, cron or systemd, that runs `./agoranomos backup`
* A DAG of Airflow

## Decision Outcome

Chosen option: "A long-running service on the PostgreSQL image of the stack …", because it needs no image and no step on the host, its `pg_dump` has the version of the servers, and its health check shows when backups stop.

In practice this means:

* **The `ops` profile** runs the `backup` service, and `./agoranomos init --server` turns it on with server mode. `./agoranomos backup` takes a backup now, in either mode, in the running service or in a container of its own.
* **When.** Every day at `BACKUP_TIME`, 03:00 by default, in the time zone `BACKUP_TZ`, UTC by default; at once when there is no scheduled backup yet; a run that failed is tried again an hour later. The service waits in a loop of the shell and runs `postgres/backup.sh`. No DAG of the base package runs on a time schedule (the load of the demo runs at the first start and by hand, and its dbt run follows the asset of the load), so 03:00 meets none of them; a DAG of the base package that gets a time schedule keeps at least an hour away from 03:00 UTC.
* **Locks.** `pg_dump` takes a shared lock on every table of its database and holds it until the dump ends. It blocks no reader and no writer, but a change of a table waits behind it, and the reads of that table that come after the change wait with it: dbt replaces a mart by a rename, and dlt alters a table when its schema grows, and `loader` and `transformer` wait at most a minute for a lock (`postgres/warehouse.sql`), so such a step that meets a dump longer than a minute fails, and its retries come later. A dump that meets such a lock waits up to five minutes (`--lock-wait-timeout`) and then fails, and the schedule tries again an hour later. The documentation says to put `BACKUP_TIME` where no pipeline runs.
* **What.** A folder named after the start of the backup in UTC, such as `20261011T030012Z`: the dumps of `warehouse`, `airflow` and `metabase` (`pg_dump`, custom format, zstd), the global objects of each instance without passwords, for reference only, since `db-init` creates the roles from `.env`, an archive of the extensions folder, `SHA256SUMS`, and `manifest.json` with the times, the images of the checkout and each file with its size and SHA-256. Not in it: `.env`, whose keys belong in a password manager, and the certificates of Caddy, which Caddy gets again. Each dump is a snapshot of its own database, one after the other: they are not of the same instant, and the pipelines, which are safe to run again, catch up.
* **Who.** The role `backup`, on each instance: a member of `pg_read_all_data`, with `CONNECT` on the databases of the stack that the instance holds and on no other, in read-only sessions, with a password of its own in `.env`. It reads every table, the hashes of the passwords of the roles too, and writes nothing.
* **Where.** `AGORANOMOS_BACKUP_DIR`, `./backups` by default, which git ignores. The service runs as `AGORANOMOS_UID:AGORANOMOS_GID`, which `./agoranomos init` sets to the user who runs it, never as root; the folder has mode 700 and every file mode 600. A backup is written as `.partial-<name>` and renamed when it is complete.
* **How long.** Of the scheduled backups, the newest of each of the last 7 days, 4 weeks and 3 months that have one stays, by the names; the three numbers are settings. A backup taken by `./agoranomos backup` is marked as taken by hand in its manifest, and no retention removes it: it is the backup before an upgrade, the only way back from a migration of Metabase. `./agoranomos status` lists the backups by hand with their size and age, and the person deletes one when it is no longer needed.
* **Health.** The service is unhealthy when the last good backup, scheduled or by hand, finished more than 26 hours ago, or when there is none 26 hours after the service started. `./agoranomos up` then warns and goes on, and `./agoranomos status` says when the last good backup was and whether the last run failed.
* **Restore.** `./agoranomos restore <backup>`, or `latest` for the last good backup, runs `preflight`; checks every file against its SHA-256 and reads each dump that it restores to its end before it stops anything; compares the major version of PostgreSQL and the tags of Airflow and Metabase with those of the checkout, and stops when they differ, unless `--force`; asks, or takes `--yes`; removes a proxy or a backup container that a mode left behind, as `up` does; stops every service but the databases; runs `db-init`; restores each database with `pg_restore --clean --if-exists --create --exit-on-error` as its superuser, inside the container of the database, through its socket; runs `ANALYZE`; and starts the stack. `--only` restores some databases, such as `metabase` before going back to an earlier release, and `metabase,airflow` when the same update also moved the Airflow image, whose database migrates one way too (`git diff <from> HEAD -- airflow/Dockerfile | grep '^[-+]FROM'` prints the old and the new image line when it did, and nothing otherwise). A failure after the stop says which databases are back, that the services are stopped, and what to do.
* **Copies elsewhere.** The documentation shows how to copy the backups to another machine with rsync or rclone. Encrypted copies elsewhere, regular checks of the restore and point-in-time recovery for big warehouses are planned for Phase 5.

The other options were not chosen for these reasons:

* **supercronic** would need a second image of our own, with a pinned binary that Renovate does not see without a rule of its own.
* **A timer of the host** is a step outside the one command, and no container would show in its health that backups stopped.
* **A DAG** would give tasks the passwords of the databases, and the DAGs of the extensions folder run there too ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)).
* **Dumps as the superusers** would give the job the power to change and drop every database.
* **A named volume** would hide the backups from the person, and copying them would need an export.

### Consequences

* Good, because backups need no step on the host, and they are plain files of the user, which any tool can copy.
* Good, because a damaged backup is found before a restore drops anything.
* Good, because a backup before an upgrade is one command, and so is the way back.
* Bad, because the role `backup` reads all the data and the hashes of the passwords of the roles, and a backup holds the hashes of the passwords of the users of Metabase: the backups folder needs the care of `.env`.
* Bad, because logical dumps grow slow and big with big warehouses, and a restore loses what changed after the backup.
* Bad, because the service takes 256 MiB of the memory budget for a job that sleeps most of the day ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)).
* Bad, because the three dumps are not of the same instant.
* Bad, because a dump makes the changes of tables by dbt and dlt wait, and fail after a minute, while it runs, and the reads that come after such a change wait with it: backups need a time when no pipeline runs.
* Bad, because the backups taken by hand stay until someone deletes them, and take disk.
* Bad, because rootless Docker and `userns-remap` are not supported: the user of the backup service would map to another user of the host.
* Bad, because a restore needs the `.env` of the deployment that made the backup, for its keys.

## More Information

* **Implementation status, as of October 2026:** in place for v0.1.0: the `backup` service of the `ops` profile, `postgres/backup.sh`, the role `backup`, `./agoranomos backup`, `restore` and the status of the backups, and the round trip in the job `server` of the Stack workflow, with a restore of Metabase into the running stack and a restore on empty volumes. Not done yet: the guide Back up and restore, with copies elsewhere, and the check on a real server.
* Other self-hosted projects keep the keys apart from their backups and restore onto the same version: [GitLab](https://docs.gitlab.com/administration/backup_restore/backup_gitlab/), [Sentry](https://develop.sentry.dev/self-hosted/backup/).
* PostgreSQL 18: [pg_dump](https://www.postgresql.org/docs/18/app-pgdump.html), [pg_restore](https://www.postgresql.org/docs/18/app-pgrestore.html), [pg_dumpall](https://www.postgresql.org/docs/18/app-pg-dumpall.html), [predefined roles](https://www.postgresql.org/docs/18/predefined-roles.html).
