# Back up and restore

A backup of Agoranomos holds its three databases, the warehouse, Airflow and Metabase, and its extensions folder ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md)). This guide takes one, schedules them, copies them to another machine, and restores them, also on a new server.

## What a backup holds

Each backup is a folder of the backups folder, `./backups` unless `AGORANOMOS_BACKUP_DIR` says otherwise, named after the time of its start in UTC, such as `20261011T030012Z`. The commands of this guide write `backups/` and `extensions/`: use your folders if you changed `AGORANOMOS_BACKUP_DIR` or `AGORANOMOS_EXTENSIONS_DIR`.

| File | What |
|---|---|
| `warehouse.dump`, `airflow.dump`, `metabase.dump` | The databases, as `pg_dump` writes them, compressed |
| `globals-warehouse-db.sql`, `globals-platform-db.sql` | The roles of the two instances, without passwords, for reference: `./agoranomos up` creates the roles from `.env` |
| `extensions.tar.gz` | The extensions folder |
| `SHA256SUMS`, `manifest.json` | The checksums; the times, the images of the stack, and whether the schedule took the backup or a person did |

The file `latest.json`, next to the folders, says how the last run went.

A backup does not hold `.env`. Its keys decrypt the connections that Airflow and Metabase save, so keep a copy of `.env` in a password manager, as [Start the stack](deploy-on-a-server.md#start-the-stack) shows, and save a new copy whenever `.env` changes, after every update too: a backup without the keys restores the data, but not the saved connections, and a copy from before a change of a password or of the admin would not match the databases. It does not hold the certificates of server mode either; Caddy gets them again. The three dumps are taken one after the other, so they are not of the same instant; the pipelines are safe to run again and catch up.

A backup holds everything that the databases hold, also the hashes of the passwords of the users of Metabase. The folder and its files are readable only by the user who runs `./agoranomos`: keep copies as safe as `.env`.

## Take a backup now

```bash
./agoranomos backup
```

It works in local mode and in server mode, once the databases run, and prints the name of the backup and its size. Take one before every upgrade, and before you update the checkout with `git pull`: a backup records the images of the checkout as they are when it runs, and `restore` compares them later. Note the commit that you run, which `git rev-parse HEAD` prints: a restore needs the checkout at it. While a scheduled backup runs, it waits for it, up to 10 minutes.

A backup that you take this way stays until you delete it: the retention removes scheduled backups only, so the backup before an upgrade is still there when you need to go back. `./agoranomos status` lists these backups with their size and age. To delete one:

```bash
rm -r backups/20261011T030012Z
```

## Back up every day

In server mode, `./agoranomos init --server` turns on the `ops` profile, which takes a backup every day. In local mode, add `ops` to `COMPOSE_PROFILES` in `.env` and run `./agoranomos up`; to turn the daily backups off, take `ops` out and run `./agoranomos up` again, which removes the container that is left, and the backups stay. The settings are in [Configuration](../reference/configuration.md#backups): the time and the time zone, and how many scheduled backups stay, the newest of each of the last 7 days, 4 weeks and 3 months by default, counted in UTC. The first backup comes as soon as the service starts.

Give the backup a time when no pipeline runs: set `BACKUP_TIME`, as `HH:MM`, and, if it is not in UTC, `BACKUP_TZ` in `.env`; a change takes effect at the next `./agoranomos up`, which starts the `backup` service again. A dump holds a shared lock on every table of its database until it ends. It blocks no query and no load of rows, but a change of a table waits behind it: dbt replaces a mart by a rename, and dlt alters a table when a source gets a new column, and both wait at most a minute for a lock and then fail; run them again, or let their next run catch up. A dump that meets such a change waits up to five minutes and then fails, and the schedule tries again an hour later. No DAG of the base package runs at 03:00 UTC, the default. `./agoranomos logs backup` shows how long each backup took.

`./agoranomos status` says when the last good backup was. It warns when the last run failed, and, with the `ops` profile, when there is no good backup in the last 26 hours; without `ops` it only says how old the last one is, because backups by hand come when you take them. `./agoranomos up` warns, and still starts the stack, when the `backup` service is unhealthy, which means the same, and when it is restarting or has exited, so that there are no new backups. `./agoranomos logs backup` shows each run. Nothing but these warnings tells you that the backups have stopped, so look at `./agoranomos status` the day after the first start, and regularly after that; `status` prints the free space of the disk of the backups folder, and `du -sh backups` shows how much disk the backups take. By default the folder is on the disk that holds the data of Docker: on a small server set `AGORANOMOS_BACKUP_DIR` to another disk, such as `/mnt/backups` ([Configuration](../reference/configuration.md#backups)). On a full disk every backup fails, and the retention, which runs after a good backup, removes none: delete old backups by hand, as [above](#take-a-backup-now).

## Copy the backups to another machine

A backup on the server is lost with the server. Copy the backups folder elsewhere every day, a little after the time of the backup. A folder that starts with `.partial-` is a backup that is still being written: leave it out. This section copies with rsync to another server over SSH, and then with rclone.

**Set up the copy.** On the other machine, rsync is installed, and a user `backup` owns an empty folder of its own, which you make as root there: `install -d -m 700 -o backup -g backup /srv/agoranomos-backups`. On the server, as `agoranomos`, make a key for the job, without a passphrase so that cron can use it, and give its public half to `backup`: `ssh-copy-id -i ~/.ssh/id_ed25519.pub backup@other-server` if that machine accepts a password once, or add the content of `~/.ssh/id_ed25519.pub` to `/home/backup/.ssh/authorized_keys` there. Whoever controls the server can then write into that folder, so give `backup` no other rights.

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
```

**Run it once by hand**, as `agoranomos` on the server. It asks you to trust the host key of the other machine, which cron could not answer, and it shows that a backup arrives, which `ls -t /srv/agoranomos-backups` on the other machine confirms. The path of the source is absolute, as it must be in cron:

```bash
rsync -a --exclude='.partial-*' /home/agoranomos/agoranomos/backups/ backup@other-server:/srv/agoranomos-backups/
```

**Run it every day** with `crontab -e`, as `agoranomos`, in the time zone of the server (`timedatectl` shows it), after the backup has ended. The line writes the output of rsync into a log file, and a date after each run that worked:

```text
30 4 * * * rsync -a --exclude='.partial-*' /home/agoranomos/agoranomos/backups/ backup@other-server:/srv/agoranomos-backups/ >> /home/agoranomos/backup-copy.log 2>&1 && date -u >> /home/agoranomos/backup-copy.log
```

`tail /home/agoranomos/backup-copy.log` shows the date of the last run that worked, and the message of rsync after one that failed; rsync prints nothing when it succeeds. Look at it after the first night. Do not count on cron to mail the output: a fresh server usually has no mail server, so the log is where the result is.

**With rclone**, to an SFTP storage such as a Hetzner Storage Box, install rclone on the server as [its installation page](https://rclone.org/install/) says (as of October 2026: its install script, Homebrew or MacPorts on a Mac, or a precompiled binary; the package of a Linux distribution is often older), and make the remote `storagebox` first, as `agoranomos`, with `rclone config`, of the type `sftp`. A Hetzner Storage Box listens for SFTP on port 23 (`port = 23` in the remote) and wants paths without a leading slash (as of October 2026, the documentation of rclone and of Hetzner). Run the copy once by hand, then in cron with the full path of `rclone`, which `command -v rclone` prints, since cron has a short `PATH`:

```bash
rclone copy --exclude "/.partial-*/**" /home/agoranomos/agoranomos/backups storagebox:agoranomos-backups
```

```text
30 4 * * * /usr/bin/rclone copy --exclude "/.partial-*/**" /home/agoranomos/agoranomos/backups storagebox:agoranomos-backups >> /home/agoranomos/backup-copy.log 2>&1 && date -u >> /home/agoranomos/backup-copy.log
```

The copies hold everything that the databases hold: send them only to storage that you control, or encrypt them before they leave the server, for example with the `crypt` remote of rclone ([rclone crypt](https://rclone.org/crypt/), as of October 2026), which encrypts the content and the names of the files. `rclone config` makes a remote of the type `crypt` over `storagebox:agoranomos-backups`; use it, as `<crypt remote>:`, in place of `storagebox:agoranomos-backups` in every rclone command of this guide, and `rclone copy` to it encrypts on the way out. Keep the password of the crypt remote, and its salt if you set one, in the password manager with `.env`: without them the copy cannot be read, and a restore from it is not possible. Encrypted copies elsewhere, built into Agoranomos, are planned for a later release.

None of these commands deletes anything at the destination, on purpose: with `rsync --delete` or `rclone sync`, a server that is rebuilt, or a `backups/` that is lost and recreated empty, would empty the copy that the restore needs. So the copy keeps what the retention removes on the server, it grows, and a full destination makes the next copy fail, which under a timer nobody sees: look at the log of the job, `backup-copy.log` above, and at the free space of the copy.

You delete old copies by hand. On another server, `find /srv/agoranomos-backups -maxdepth 1 -name '20*' -mtime +100` lists the backups that are older than 100 days, which is longer than the 3 months that the server keeps by default, and `rm -r /srv/agoranomos-backups/20260901T030012Z` deletes one. With rclone, `rclone lsd storagebox:agoranomos-backups` lists them with their times, and `rclone purge --dry-run storagebox:agoranomos-backups/20260901T030012Z` shows what a purge of one would remove; run it again without `--dry-run` to remove it. The path must end in the name of a backup: if you leave the name out, `rclone purge` removes the whole copy. A copy job that has stopped makes every backup look old, so check that new copies arrive before you delete any, and keep the backup before an upgrade for as long as you may go back to it.

## Restore

Restore with the release that took the backup. If the checkout is already at the commit that you ran then, as when you restore on the same server after a failure and no update, go on. If it is not, check that commit out: the one that you noted with `git rev-parse HEAD` before the upgrade. The manifest of a backup records the images and the times, not the commit, so note the commit in the password manager with `.env` at every upgrade; if you did not, `git reflog` shows where the checkout was before a `git pull`, and on a new server the message of [`restore`](../reference/command-line.md#restore) names the versions that the backup has, and `git log -S'<the tag it names>' -- compose.yaml airflow/Dockerfile` lists the commit that replaced the tag and, below it, the one that added it: check out the lower one, or a commit between the two, and `restore` says again if a version still differs. `restore` compares what the backup records with the checkout, for the databases it restores: the major version of PostgreSQL and the versions of Airflow and Metabase. It stops when they differ.

```bash
git checkout <commit>   # only if the checkout is at another commit
./agoranomos restore 20261011T030012Z
```

When you update again, `git checkout main` returns the checkout to the branch: `git pull` does not work on a detached `HEAD`.

`latest` names the last good backup. `restore` replaces the databases it restores, so if the current ones still hold something that you may need, take a backup of them first with `./agoranomos backup`, and take no backup by hand while a restore runs: it could dump a database that is half restored. `restore` needs the `.env` of the deployment that took the backup: its keys decrypt the connections that Airflow and Metabase saved.

Before it asks or changes anything, `restore` runs the checks of `preflight`, without `--force`: on a machine where `up` needs `--force`, because Docker has less memory or disk than the minimum, `restore` stops too, and its own `--force` is about the images only. On Linux it also stops when the backups folder belongs to another user than `AGORANOMOS_UID`, and on `AGORANOMOS_UID=0`. It then checks the backup: every file against its SHA-256, and each dump that it restores read to its end. While a backup runs, the check waits for it, up to 10 minutes, and while the check runs, a scheduled backup that comes due fails and is tried again an hour later. It compares the images, and stops when they differ, unless `--force`, which restores a backup of other versions after it has printed the differences: use it only when you know that the versions of the checkout read it. On a terminal it then asks you to type `yes`, on standard error; elsewhere it needs `--yes`, which replaces the question in a script. A refused restore, whether the check, the images or your answer refuses it, changes nothing.

Only after the answer does it change anything: it stops and removes a `proxy` or `backup` container that a mode left behind, as `up` does, stops every service but the databases, runs `db-init`, restores each database as it was at the backup, runs `ANALYZE`, and starts the stack. It works on a running stack and on one that is down. While it runs, the site is down, as the proxy and the `backup` service are among the services that it stops: about 40 seconds for the demo data on a Mac, measured on 11 October 2026. Everything saved after the backup is lost.

To restore some databases only, name them in one `--only`, separated by commas and each once:

```bash
./agoranomos restore 20261011T030012Z --only metabase
```

If a restore fails, or you interrupt it with Ctrl-C, after it stopped the services, it says which databases it restored and which not, that the services are stopped, and what to do. When no database was restored, `./agoranomos up` starts the stack again as it was. When a database may be incomplete, do not run `up`, which would start the stack on it: restore again first. When `pg_restore` says that the database is being accessed by other users, that database was not changed: stop what is connected and run `restore` again. [Command line](../reference/command-line.md#restore) lists the options.

A scheduled backup near the end of its retention can be removed between the check and the copy, so restore such a backup away from `BACKUP_TIME`, or copy the backup out of the backups folder by hand first. A restore needs free disk for Docker of the size of the biggest dump, which it copies into the container of its database for the time of the restore. It also runs inside the memory limit of that database: before you restore a warehouse of several gigabytes, raise `WAREHOUSE_DB_MEMORY` in `.env`, as [Give a service more memory](run-locally.md#give-a-service-more-memory) shows: `restore` starts the databases again, with the limit of `.env`. `restore` leaves `extensions.tar.gz` as it is; to put the extensions folder back (`extensions/` unless `AGORANOMOS_EXTENSIONS_DIR` says otherwise):

```bash
tar -xzf backups/20261011T030012Z/extensions.tar.gz -C extensions
```

### Go back from an upgrade

An upgrade of Metabase migrates its database one way, and the backup that you took before it is the way back: restore `--only metabase`. When the same update also changed the Airflow image, which migrates the database of Airflow one way too, restore `--only metabase,airflow`, or `airflow-init` fails on the schema of the newer Airflow; you lose the runs of Airflow since the backup. See whether the image changed before you return the checkout, while it is still at the new release:

```bash
git diff <the commit that you came from> HEAD -- airflow/Dockerfile compose.yaml
git checkout <the commit that you came from>
./agoranomos restore 20261011T030012Z --only metabase
# when the Airflow image changed as well:
./agoranomos restore 20261011T030012Z --only metabase,airflow
```

If `restore` stops because the images of the backup differ from those of the checkout, the checkout is not at the commit that you came from ([Configuration](../reference/configuration.md#back-up-metabase-before-an-upgrade)).

## Restore on a new server

1. Set up the new server as [Deploy on a server](deploy-on-a-server.md) says: [Prepare the server](deploy-on-a-server.md#prepare-the-server) and [Open ports 22, 80 and 443 only](deploy-on-a-server.md#open-ports-22-80-and-443-only). Then, as `agoranomos`, run the first two commands of its [Start the stack](deploy-on-a-server.md#start-the-stack), and check out the commit that took the backup ([Restore](#restore) says how to find it). Do not run its third command, `./agoranomos init --server`, nor `up`: the `.env` comes from the password manager in step 2, and `init` runs in step 3. Point the A record of the domain at the new server.

    ```bash
    git clone https://github.com/agoranomos/agoranomos.git
    cd agoranomos
    git checkout <commit of the backup>
    ```

2. Put `.env` into the checkout: save the attachment of the password manager to your own machine as `env-copy`, run `scp env-copy agoranomos@<new-server>:agoranomos/.env`, and delete `env-copy`; or open `.env` in an editor on the server, such as `nano .env`, and paste the text. Do not use `cat` or a pasted here-document: bash keeps what you type in its history.
3. In `.env`, set `AGORANOMOS_UID=auto` and `AGORANOMOS_GID=auto`, and run `./agoranomos init`, which puts in the IDs of the user of the new server and makes `backups/` its own; with a new domain name, run `./agoranomos init --server <domain>`. The `.env` of another machine keeps the IDs of that one, and `preflight` stops on them with the `ops` profile; on Linux, `restore` stops on them whenever the backups folder belongs to another user.
4. Copy the backup that you restore into `backups/`: its folder, and `latest.json` too if you restore `latest`. Do not copy the whole copy: it has grown with every backup, and may not fit next to the data of Docker. From the machine that holds the copies, with your own key, which the new server accepts: `rsync -a /srv/agoranomos-backups/20261011T030012Z agoranomos@<new-server>:agoranomos/backups/`. With rclone, on the new server, after `rclone config` has made the same remotes as in [Copy the backups to another machine](#copy-the-backups-to-another-machine), with the same crypt password and salt: `rclone copy <crypt remote>:20261011T030012Z backups/20261011T030012Z` if the copies are encrypted, or `rclone copy storagebox:agoranomos-backups/20261011T030012Z backups/20261011T030012Z` if they are not, followed by `chmod -R go-rwx backups`. The files belong to the user who copied them. If you copied as another user, give them to the user of the stack as root (`ssh root@<new-server>`): `chown -R agoranomos:agoranomos /home/agoranomos/agoranomos/backups`.
5. Run `./agoranomos preflight`, which checks Docker, the memory, the disk and the ports of the new server. `restore` runs the same checks without `--force`, so a server below the minimum of [System requirements](../reference/system-requirements.md) stops it, and its own `--force` does not change that: size an emergency server for the minimum at least.
6. Put the extensions folder back: `tar -xzf backups/20261011T030012Z/extensions.tar.gz -C extensions`.
7. Run `./agoranomos restore 20261011T030012Z`. It starts the databases on empty volumes, restores them, and starts the stack.

Sign in to Metabase with the password of the old deployment, which is in the `.env` that you copied.

## Limits

Backups are logical dumps, which suit warehouses of a few gigabytes. A big warehouse takes long to dump and to restore, and a restore loses what changed after the backup; incremental backups with a restore to a point in time are planned for a later release. Nothing checks by itself that a restore works: try one on a spare machine before you need it. The backups need Docker as root, the default of Docker Engine: rootless Docker and `userns-remap` are not supported, because the backups would belong to another user of the host.
