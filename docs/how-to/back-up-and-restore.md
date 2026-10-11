# Back up and restore

A backup of Agoranomos holds its three databases, the warehouse, Airflow and Metabase, and its extensions folder ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md)). This guide takes one, schedules them, copies them to another machine, and restores them, also on a new server.

## What a backup holds

Each backup is a folder of `./backups`, named after the time of its start in UTC, such as `20261011T030012Z`:

| File | What |
|---|---|
| `warehouse.dump`, `airflow.dump`, `metabase.dump` | The databases, as `pg_dump` writes them, compressed |
| `globals-warehouse-db.sql`, `globals-platform-db.sql` | The roles of the two instances, without passwords, for reference: `./agoranomos up` creates the roles from `.env` |
| `extensions.tar.gz` | The extensions folder |
| `SHA256SUMS`, `manifest.json` | The checksums; the times, the images of the stack, and whether the schedule took the backup or a person did |

The file `latest.json`, next to the folders, says how the last run went.

A backup does not hold `.env`. Its keys decrypt the connections that Airflow and Metabase save, so keep a copy of `.env` in a password manager: a backup without it restores the data, but not the saved connections. It does not hold the certificates of server mode either; Caddy gets them again. The three dumps are taken one after the other, so they are not of the same instant; the pipelines are safe to run again and catch up.

A backup holds everything that the databases hold, also the hashes of the passwords of the users of Metabase. The folder and its files are readable only by the user who runs `./agoranomos`: keep copies as safe as `.env`.

## Take a backup now

```bash
./agoranomos backup
```

It works in local mode and in server mode, once the databases run, and prints the name of the backup and its size. Take one before every upgrade, and before you update the checkout with `git pull`: a backup records the images of the checkout as they are when it runs, and `restore` compares them later. While a scheduled backup runs, it waits for it, up to 10 minutes.

A backup that you take this way stays until you delete it: the retention removes scheduled backups only, so the backup before an upgrade is still there when you need to go back. `./agoranomos status` lists these backups with their size and age. To delete one:

```bash
rm -r backups/20261011T030012Z
```

## Back up every day

In server mode, `./agoranomos init --server` turns on the `ops` profile, which takes a backup every day. In local mode, add `ops` to `COMPOSE_PROFILES` in `.env` and run `./agoranomos up`; to turn the daily backups off, take `ops` out and run `./agoranomos up` again, which removes the container that is left, and the backups stay. The settings are in [Configuration](../reference/configuration.md#backups): the time and the time zone, and how many scheduled backups stay, the newest of each of the last 7 days, 4 weeks and 3 months by default, counted in UTC. The first backup comes as soon as the service starts.

Give the backup a time when no pipeline runs. A dump holds a shared lock on every table of its database until it ends. It blocks no query and no load of rows, but a change of a table waits behind it: dbt replaces a mart by a rename, and dlt alters a table when a source gets a new column, and both wait at most a minute for a lock and then fail; run them again, or let their next run catch up. A dump that meets such a change waits up to five minutes and then fails, and the schedule tries again an hour later. No DAG of the base package runs at 03:00 UTC, the default. `./agoranomos logs backup` shows how long each backup took: the dump of a warehouse of a few gigabytes takes minutes.

`./agoranomos status` says when the last good backup was. It warns when the last run failed, and, with the `ops` profile, when there is no good backup in the last 26 hours; without `ops` it only says how old the last one is, because backups by hand come when you take them. `./agoranomos up` warns when the `backup` service is unhealthy, which means the same, and still starts the stack. `./agoranomos logs backup` shows each run. On a full disk every backup fails, and the retention, which runs after a good backup, removes none: delete old backups by hand, as [above](#take-a-backup-now).

## Copy the backups to another machine

A backup on the server is lost with the server. Copy the backups folder elsewhere every day, from a timer of the host or by hand, a little after the time of the backup. A folder that starts with `.partial-` is a backup that is still being written: leave it out. For example, with rsync to another server over SSH:

```bash
rsync -a --delete --exclude='.partial-*' ./backups/ backup@other-server:/srv/agoranomos-backups/
```

or with [rclone](https://rclone.org/) to an SFTP storage, such as a Hetzner Storage Box:

```bash
rclone sync --exclude "/.partial-*/**" ./backups storagebox:agoranomos-backups
```

For rclone, make the remote `storagebox` first with `rclone config`, of the type `sftp`; a Hetzner Storage Box listens for SFTP on port 23 (`port = 23` in the remote) and wants paths without a leading slash (as of October 2026, the documentation of rclone and of Hetzner). `rclone sync` makes the destination equal to the source: try it with `--dry-run` first.

The copies hold everything that the databases hold: send them only to storage that you control, or encrypt them on the way. With `--delete` and `sync`, a backup that you delete on the server goes from the copy too. Encrypted copies elsewhere, built into Agoranomos, are planned for a later release.

## Restore

Restore with the release that took the backup: check out the commit that you ran then (`git rev-parse HEAD` prints it; note it before an upgrade). `restore` compares what the backup records with the checkout, for the databases it restores: the major version of PostgreSQL and the versions of Airflow and Metabase. It stops when they differ.

```bash
./agoranomos restore 20261011T030012Z
```

`latest` names the last good backup. `restore` replaces the databases it restores, so if the current ones still hold something that you may need, take a backup of them first with `./agoranomos backup`.

`restore` works on a running stack and on one that is down. It first runs the checks of `preflight`, then checks the backup, every file against its SHA-256 and each dump read to its end, and changes nothing if a check fails. It then asks you to type `yes`, stops every service but the databases, restores each database as it was at the backup, and starts the stack. Everything saved after the backup is lost. If a restore fails, it names the databases that it restored before the failure: fix the cause and run it again, and do not start the stack until a restore has gone through. To restore some databases only, such as Metabase before going back to an earlier release, name them, separated by commas:

```bash
./agoranomos restore 20261011T030012Z --only metabase
```

In a script, `--yes` replaces the question; outside a terminal, `restore` needs it. `--force` restores a backup of other versions after it has printed the differences: use it only when you know that the versions of the checkout read it. [Command line](../reference/command-line.md) lists the options.

A restore needs free disk for Docker of the size of the biggest dump, which it copies into the container of its database for the time of the restore. It also runs inside the memory limit of that database: before you restore a warehouse of several gigabytes, raise `WAREHOUSE_DB_MEMORY` in `.env` before you run `restore`, as [Give a service more memory](run-locally.md#give-a-service-more-memory) shows: `restore` starts the databases again, with the limit of `.env`. `restore` leaves `extensions.tar.gz` as it is; to put the extensions folder back:

```bash
tar -xzf backups/20261011T030012Z/extensions.tar.gz -C extensions
```

## Restore on a new server

1. Set up the new server as [Deploy on a server](deploy-on-a-server.md) says, up to the clone of the release that took the backup, or its copy with `git archive`, but do not run `./agoranomos init` or `up` yet. Point the A record of the domain at the new server.
2. Copy `.env` from the password manager into the checkout.
3. In `.env`, set `AGORANOMOS_UID=auto` and `AGORANOMOS_GID=auto`, and run `./agoranomos init`, which puts in the IDs of the user of the new server and makes `backups/` its own; with a new domain name, run `./agoranomos init --server <domain>`. The `.env` of another machine keeps the IDs of that one, and `preflight` stops on them.
4. Copy the backup into `backups/`, such as from the machine that holds the copies: `rsync -a backup@other-server:/srv/agoranomos-backups/ backups/`, which brings `latest.json` too. The files keep the owner that the copy gave them: `chown -R` them to the user of the stack if they belong to another one.
5. Run `./agoranomos preflight`, which checks Docker, the memory, the disk and the ports of the new server.
6. Put the extensions folder back: `tar -xzf backups/20261011T030012Z/extensions.tar.gz -C extensions`.
7. Run `./agoranomos restore 20261011T030012Z`. It starts the databases on empty volumes, restores them, and starts the stack.

Sign in to Metabase with the password of the old deployment, which is in the `.env` that you copied.

## Limits

Backups are logical dumps, which suit warehouses of a few gigabytes. A big warehouse takes long to dump and to restore, and a restore loses what changed after the backup; incremental backups with a restore to a point in time are planned for a later release. Nothing checks by itself that a restore works: try one on a spare machine before you need it. The backups need Docker as root, the default of Docker Engine: rootless Docker and `userns-remap` are not supported, because the backups would belong to another user of the host.
