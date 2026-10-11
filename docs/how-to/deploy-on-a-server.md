# Deploy on a server

This guide puts Agoranomos on a server of your own in server mode: Metabase on the internet over HTTPS with a certificate of Let's Encrypt, Airflow behind an SSH tunnel, and a backup of the databases every day ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md), [ADR-0020](../adr/0020-back-up-the-databases-every-day.md)). The example is a server of Hetzner Cloud with Ubuntu; any Linux server with Docker works the same way.

!!! note "Pre-release"

    Server mode and the backups run from the `main` branch, and v0.1.0 is to be their first release. The repository is private until v0.3, so cloning it needs an invitation.

## Before you begin

- A Linux server, arm64 or x86-64 with x86-64-v2, with 8 GB of memory, 4 CPUs and 20 GB of free disk, the recommended resources of [System requirements](../reference/system-requirements.md); the backups take disk too, and the CeleryExecutor variant needs 12 GB. On Hetzner Cloud, a CAX21 (arm64, 4 vCPU, 8 GB, 80 GB of disk) costs $12.49 a month, as of October 2026.
- Ubuntu 24.04 LTS or 26.04 LTS on it, and SSH access as root with your key, which is how a new server of Hetzner Cloud starts.
- A domain name for Metabase, such as `bi.example.com`, with an A record that points at the IPv4 address of the server. If the domain is at Cloudflare, set the record to "DNS only": Caddy, on the server, gets the certificate itself.

!!! warning "IPv4 only for now"

    Do not give the domain an AAAA record yet. The networks of Docker have no IPv6, which Docker Engine does not turn on by default, and Docker is then expected to hand the connections of IPv6 clients to the proxy from an address of its own, so that Caddy and Metabase would see one address for every IPv6 client: the limit of Metabase on failed sign-ins from one address would count all IPv6 clients together, and the logs would not show who connected ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md)). A check on a real server is to settle this before v0.1.0.

Rootless Docker and `userns-remap` are not supported: they map the users of containers to other users of the host, and the backups would belong to one of those ([Configuration](../reference/configuration.md#backups)).

## Prepare the server

As root on the server, `ssh root@<server>`, install Docker Engine with its Compose plugin from Docker's own repository, as [Docker's guide for Ubuntu](https://docs.docker.com/engine/install/ubuntu/) says (read on 11 October 2026), and git. Server mode needs Docker Engine 28.0 or newer: before 28.0, other hosts of the same network segment can reach the ports that Docker publishes on `127.0.0.1`, and `./agoranomos preflight` stops in server mode.

```bash
apt update
apt install ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
apt update
apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Then create the user of the stack, who runs `./agoranomos` and owns the backups, since `preflight` refuses to write them as root. It joins the group `docker`, and your SSH key signs it in:

```bash
useradd --create-home --shell /bin/bash agoranomos
usermod -aG docker agoranomos
install -d -m 700 -o agoranomos -g agoranomos /home/agoranomos/.ssh
install -m 600 -o agoranomos -g agoranomos /root/.ssh/authorized_keys /home/agoranomos/.ssh/authorized_keys
```

Sign out, and sign in as that user, `ssh agoranomos@<server>`: the rest of this guide runs as `agoranomos`. A member of the group `docker` controls Docker, and so the server: keep this user for the stack.

## Open ports 22, 80 and 443 only

Docker publishes ports around the rules of UFW: a port that a container publishes on every address is open whatever UFW says ([Docker and ufw](https://docs.docker.com/engine/network/packet-filtering-firewalls/#docker-and-ufw), read on 11 October 2026). Agoranomos publishes TCP 80 and 443 on every address, and everything else on `127.0.0.1` only, but a firewall outside the server is what keeps the rest closed, also the ports of what else runs there.

On Hetzner Cloud, a Cloud Firewall is free, and you set it up in the project and not on the server, so the rules that Docker adds there do not change it. It drops the inbound traffic that no rule allows. In the Hetzner Console, open **Firewalls** and **Create Firewall**, add inbound rules for TCP 22, TCP 80 and TCP 443 from any IPv4 and IPv6 address, and under **Apply to** choose the server. With the `hcloud` CLI, the same from a file of rules, which you save as `firewall.json` in the folder where you run the commands. `hcloud` runs on your own machine, not on the server: install it and make a context for your project as [Hetzner's setup guide](https://github.com/hetznercloud/cli/blob/main/docs/tutorials/setup-hcloud-cli.md) says, for example `brew install hcloud` and `hcloud context create <name>`, which asks for the API token of the project at a prompt (as of October 2026):

```json
[
  {"direction": "in", "protocol": "tcp", "port": "22", "source_ips": ["0.0.0.0/0", "::/0"]},
  {"direction": "in", "protocol": "tcp", "port": "80", "source_ips": ["0.0.0.0/0", "::/0"]},
  {"direction": "in", "protocol": "tcp", "port": "443", "source_ips": ["0.0.0.0/0", "::/0"]}
]
```

```bash
hcloud firewall create --name agoranomos --rules-file firewall.json
hcloud firewall apply-to-resource --type server --server <server> agoranomos
hcloud firewall describe agoranomos
```

`describe` lists the rules and the servers that the firewall is applied to: check that the inbound rules are TCP 22, 80 and 443 and nothing else, and that your server is listed. If your own address does not change, give port 22 that address alone instead of `0.0.0.0/0` and `::/0`. Port 80 stays open: Let's Encrypt checks the domain through it, and Caddy redirects it to HTTPS.

## Start the stack

On the server, as `agoranomos`:

```bash
git clone https://github.com/agoranomos/agoranomos.git
cd agoranomos
./agoranomos init --server bi.example.com
```

A server without access to the private repository can get the code from a checkout on your own machine instead. In that checkout, with the commit or the tag to deploy:

```bash
git archive --format=tar --prefix=agoranomos/ <commit> | ssh agoranomos@<server> tar -xf -
```

Then, on the server, `cd agoranomos` and `./agoranomos init --server bi.example.com`. Such a copy has no history, so `git pull` cannot update it: it suits a check of a release, and a server that you keep gets a clone.

`init` creates `.env` with new secrets, sets `AGORANOMOS_SITE_URL=https://bi.example.com`, and adds the profiles `server` and `ops`, which run the proxy and the daily backups, to `COMPOSE_PROFILES`. It puts the user ID and the group ID of the user who runs it into `AGORANOMOS_UID` and `AGORANOMOS_GID`, makes the folder `backups/` for that user alone, and sets the rules of Metabase for the internet: new and changed passwords need at least 15 characters (`METABASE_PASSWORD_COMPLEXITY=strong-enough`), and a person who does nothing for 24 hours is signed out (`METABASE_SESSION_TIMEOUT`) ([Configuration](../reference/configuration.md#server-mode)). It tells you to point the A record of the domain at the server, if you have not yet. Before the first start, open `.env` and set `AGORANOMOS_ADMIN_EMAIL` to your own address, the login of the admin of Metabase. To start without the demo, set `COMPOSE_PROFILES=core,server,ops`.

To try the setup first, without using up the certificates that Let's Encrypt issues for a name, at most five in seven days (as of October 2026), set `AGORANOMOS_ACME_CA=https://acme-staging-v02.api.letsencrypt.org/directory` in `.env` before `up`: no browser trusts the certificates of the staging directory, and `up` says so when it checks the certificate. Set it back to `https://acme-v02.api.letsencrypt.org/directory` and run `./agoranomos up` again for a real certificate.

When the edits of `.env` are done, keep a copy of it in a password manager: a restore needs its keys and its passwords, and a copy from before an edit, such as that of `AGORANOMOS_ADMIN_EMAIL`, would not match the databases. On your own machine, `(umask 077 && scp agoranomos@<server>:agoranomos/.env env-copy)` copies it; attach `env-copy` to an entry of the password manager and delete the file. Do not print `.env` with `cat` in a terminal that others can see, and do not paste it into a chat. Save a new copy whenever `.env` changes: after every update, because `init` may add settings and secrets; after a password that you changed in Metabase or Airflow and put into `.env`; and after any other edit.

```bash
./agoranomos up
```

The first start builds the images of Airflow and of the proxy and takes a few minutes. When the stack is up, `up` prints the address of the site and the command of the tunnel to Airflow. It then checks for about a minute that the site answers with a certificate that this machine trusts, and warns if it does not ([If something goes wrong](#if-something-goes-wrong)).

## Sign in

Open `https://bi.example.com` and sign in with `AGORANOMOS_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` of `.env`; `./agoranomos credentials` names both settings and never prints a password. Public links of questions and dashboards are off in a new Metabase; an admin may turn them on in Admin > Settings > Public sharing. A Metabase that was set up before server mode keeps the setting that it had, so check it there ([Configuration](../reference/configuration.md#public-links)).

Airflow stays on the server. From your own machine, open a tunnel and keep it open while you work:

```bash
ssh -L 8080:127.0.0.1:8080 agoranomos@<server>
```

Then open `http://127.0.0.1:8080` and sign in as `admin` with `AIRFLOW_ADMIN_PASSWORD`. If your own machine already uses port 8080, such as for a local Agoranomos, `ssh` only warns that it cannot listen and goes on, and the address opens the Airflow of your machine, which refuses the password of the server: choose another local port, `ssh -N -L 18080:127.0.0.1:8080 agoranomos@<server>`, and open `http://127.0.0.1:18080` (`-N` runs no command on the server, so the session is only the tunnel). Metabase is on `127.0.0.1:3000` of the server too, for a tunnel of the same kind. If you changed `AIRFLOW_PORT` or `METABASE_PORT` in `.env`, use those ports.

## What is open

| Port | On | What |
|---|---|---|
| 80, TCP | every address | Caddy: the redirect to HTTPS and the checks of Let's Encrypt |
| 443, TCP | every address | Caddy: Metabase over HTTPS |
| `AIRFLOW_PORT`, 8080 | `127.0.0.1` | Airflow, for an SSH tunnel |
| `METABASE_PORT`, 3000 | `127.0.0.1` | Metabase, for an SSH tunnel |

The databases publish no port. The proxy answers 404 to the setup API of Metabase and to the two endpoints that would forward requests to the servers of Metabase without a sign-in ([ADR-0021](../adr/0021-leave-the-outgoing-traffic-of-metabase-open.md)).

## Backups

The `ops` profile takes a backup of the three databases every day at 03:00 UTC into the backups folder, `backups/` in the checkout unless `AGORANOMOS_BACKUP_DIR` says otherwise (the commands of the guides write `backups/`: use your folder), and keeps the newest of each of the last 7 days, 4 weeks and 3 months. The first one comes as soon as the service starts, and `./agoranomos status` then says when the last good backup was. A backup that you take with `./agoranomos backup` stays until you delete it, and `status` lists it with its size and age. Nothing tells you that the backups have stopped except the warnings of `up` and `status`: look at `./agoranomos status` the day after the first start, and regularly after that. `status` also prints the free space of the disk of the backups folder, and `du -sh backups` shows how much disk they take. By default the folder is on the disk that holds the data of Docker: on a small server set `AGORANOMOS_BACKUP_DIR` to another disk, such as `/mnt/backups` ([Configuration](../reference/configuration.md#backups)). No pipeline of the base package runs at 03:00 UTC; if pipelines of yours run at night, give the backup a time apart from them, with `BACKUP_TIME` and, if it is not in UTC, `BACKUP_TZ` in `.env`, and run `./agoranomos up`, which starts the backup service again with them. A change of a table by dbt or dlt waits behind a dump and fails after a minute ([Back up and restore](back-up-and-restore.md#back-up-every-day)).

A backup on the same server is lost with the server: copy the backups to another machine, as [Back up and restore](back-up-and-restore.md#copy-the-backups-to-another-machine) shows.

## Update

Note the commit that you run now, and take a backup, before you update the checkout: the backup records the images of the checkout as they are when it runs, and a restore needs the checkout at the commit that took it:

```bash
git rev-parse HEAD   # note this commit: the way back needs it
./agoranomos backup
git pull
./agoranomos up
```

`up` adds the settings that the new release brings to `.env`, builds the images that changed and starts the services again; the data stays. Save a new copy of `.env` after the update, as [Start the stack](#start-the-stack) says, since `init` may have added settings and secrets. A release that moves Metabase to a new major version migrates its database one way, and only the backup can undo that: [Update to the latest main](run-locally.md#update-to-the-latest-main) shows how to see whether an update does, and [Configuration](../reference/configuration.md#back-up-metabase-before-an-upgrade) how to go back. To go back, see whether the update also changed the Airflow image, while the checkout is still at the new release, then check out the commit that you noted and restore the backup, `--only metabase` or `--only metabase,airflow`: [Go back from an upgrade](back-up-and-restore.md#go-back-from-an-upgrade) shows the commands. `git checkout main` returns the checkout to the branch when you update again, as `git pull` does not work on a detached `HEAD`.

## Turn server mode on or off later

`./agoranomos init --server bi.example.com` also turns server mode on for a deployment that runs in local mode. It keeps every value that is set. Before you run `./agoranomos up` again, open Metabase as it runs now and check Admin > Settings > Public sharing: a public link that someone made in local mode, which only this machine reached, is open to the internet once the site is up ([Configuration](../reference/configuration.md#public-links)). If the `METABASE_ADMIN_PASSWORD` of that deployment has fewer than 15 characters, `preflight` stops on it ([below](#if-something-goes-wrong)).

To go back to local mode, run `./agoranomos down` first, while `server` is still in `COMPOSE_PROFILES`: Compose stops only the services of the profiles that are on, so the proxy would keep serving Metabase on ports 80 and 443 after you take `server` out. Then edit `.env`: take `server` out of `COMPOSE_PROFILES`, and `ops` too unless you want the daily backups in local mode, delete the lines `AGORANOMOS_SITE_URL`, `METABASE_PASSWORD_COMPLEXITY` and `METABASE_SESSION_TIMEOUT`, and run `./agoranomos up`. If you took `server` or `ops` out first, `up` and `down` stop and remove the leftover `proxy` or `backup` container and say so; the volume of the certificates and the backups stay.

## If something goes wrong

- **`up` warns that the site does not answer with a trusted certificate.** The A record of the domain does not point at the server yet, or a firewall closes port 80 or 443, or `AGORANOMOS_ACME_CA` is the staging directory. Two records can also stop it: an AAAA record of the domain, such as a default of your registrar, because Let's Encrypt tries the IPv6 address first and goes back to IPv4 only when the connection times out, not when something else answers there ([IPv6 support](https://letsencrypt.org/docs/ipv6-support/), as of October 2026); and a CAA record that does not allow `letsencrypt.org` ([CAA](https://letsencrypt.org/docs/caa/)). `./agoranomos logs proxy` shows what Let's Encrypt answered. Caddy keeps trying by itself and waits longer and longer between its tries: after you fix the record or the firewall, `docker compose restart proxy` makes it try again at once, and `./agoranomos up` checks again. Let's Encrypt allows five failed validations for a name per account in an hour ([rate limits](https://letsencrypt.org/docs/rate-limits/), as of October 2026), so while you are still fixing it, set the staging directory ([Start the stack](#start-the-stack)) and set the real one back when a certificate comes.
- **`preflight` stops on Docker Engine older than 28.0.0.** Install Docker from Docker's repository, as [Prepare the server](#prepare-the-server) shows.
- **`preflight` stops because port 80 or 443 is in use.** Another web server runs on the server: stop it, or run Agoranomos on a server of its own.
- **`preflight` stops on `AGORANOMOS_UID` 0.** You ran `./agoranomos` as root, which left `.env`, `backups/` and `extensions/` owned by root. As root, in the checkout, run `./agoranomos down`, then give the checkout to the user of the stack: `chown -R agoranomos:agoranomos /home/agoranomos/agoranomos`; if it is under `/root`, move it first with `mv /root/agoranomos /home/agoranomos/`. Then, as `agoranomos`, set `AGORANOMOS_UID=auto` and `AGORANOMOS_GID=auto` in `.env`, and run `./agoranomos init` and `./agoranomos up`.
- **`preflight` stops because the backups folder belongs to another user than `AGORANOMOS_UID`.** The service runs as that user and cannot write into a folder of mode 700 that another user owns. It happens when you move the `.env` of a Mac or of another server to a new Linux server, which keeps the IDs of the first: set `AGORANOMOS_UID` and `AGORANOMOS_GID` to `auto` there and run `./agoranomos init`, and if `backups/` belongs to another user, give it to the user of the stack as root (`ssh root@<server>`): `chown -R agoranomos:agoranomos /home/agoranomos/agoranomos/backups` (or your backups folder).
- **`preflight` stops on `METABASE_ADMIN_PASSWORD`, which has fewer than 15 characters.** Server mode takes passwords of at least 15 characters. For a new deployment, write a longer value in `.env`, or leave it empty for `init` to generate one. A Metabase that is set up already keeps the password of its admin, so change it in Metabase first (Account settings) and then put the same value in `.env`. With the stack stopped, `docker compose up -d metabase` in the checkout starts Metabase alone, without the proxy, at `127.0.0.1` on `METABASE_PORT`, which on a server you reach through an SSH tunnel ([Configuration](../reference/configuration.md#server-mode)).
- **`preflight` warns that server mode runs without the backups.** `COMPOSE_PROFILES` has `server` and not `ops`: `./agoranomos init --server bi.example.com` adds `ops`.
- **The site answers 502.** Metabase is not up: `./agoranomos status` and `./agoranomos logs metabase`.
- **`up` or `status` warns that there is no good backup in the last 26 hours.** `./agoranomos logs backup` shows why: a full disk, a backups folder that the service cannot write, or a dump that waited more than five minutes behind a change of a table. The schedule tries again an hour after a failure.
- **`up` warns that the backup service is restarting or has exited.** It says `backup: the backup service is` and its state, `so there are no new backups`. `./agoranomos logs backup` shows why, such as a backups folder that the service cannot write; the stack works without it.
- **`status` warns that the last backup failed, and the disk is full.** While the disk is full, every backup fails, and the retention, which runs after a good backup, removes none, so the disk does not free itself. Delete the backups that you no longer need with the name of a backup, such as `rm -r backups/20261011T030012Z`: old ones that you took by hand, which `./agoranomos status` lists, and old scheduled ones, but keep the backup before an upgrade while you may still go back to it. Then run `./agoranomos backup`. `./agoranomos status` prints the free space of the disk of the backups folder, and on a small server the folder belongs on another disk ([Configuration](../reference/configuration.md#backups)).
