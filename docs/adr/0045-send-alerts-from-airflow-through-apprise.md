---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Send alerts from Airflow through Apprise

## Context and Problem Statement

The maintainer wants a deployment that runs for years with no one watching it, and reports a problem by itself. Today a person finds a problem by opening the Ops collection in Metabase or by running `./agoranomos status`. Until someone does, no one knows. A failed task is only a part of the problem: an account in quarantine, a late source and a full disk give no red task.

[ADR-0021](0021-leave-the-outgoing-traffic-of-metabase-open.md) says that the product sets up no email server, no Slack and no webhook for Metabase. An alert is an outgoing channel, so it gets a record of its own.

What notices a problem, what carries the message, and what may a message hold?

## Decision Drivers

* A task that the kernel killed for its memory is noticed too: that is how tasks fail here.
* What is not a failed task, such as a quarantine or a full disk, is noticed as well.
* Any channel that a person uses, from Telegram to Slack or email, with one line of settings.
* No new service, and little memory.
* A secret of a channel stays out of the interface of Airflow.
* A message holds no text of a platform, which anyone can write.
* With no setting, nothing leaves the deployment.

## Considered Options

For what delivers the message:

* The library Apprise, with the addresses in `.env`
* The provider of Apprise for Airflow
* Notifiers from the image of Airflow
* Alerts of Metabase on the questions of the Ops collection
* A module of our own over HTTP

For what notices a problem:

* A listener `on_dag_run_failed` in a plugin, and a DAG that watches every 15 minutes
* `on_failure_callback` on each task
* A new service

## Decision Outcome

Chosen options: "The library Apprise, with the addresses in `.env`" and "A listener `on_dag_run_failed` in a plugin, and a DAG that watches every 15 minutes", because one code serves every channel with the address out of reach of the interface of Airflow, and the listener is the one mechanism that also fires for a killed task, while the watcher sees what never fails.

In practice this means:

* **The listener.** A plugin in `airflow/plugins/`, one for all the DAGs, those of `extensions/` too. It runs in the scheduler, and is to work in both variants of the executor, of which only the LocalExecutor has been run. It sends in a background thread with a timeout, as a listener can slow a component down.
* **The watcher.** A DAG every 15 minutes reads what does not look like a failure: accounts in quarantine, late sources, the status of a backup and the used disk. It sends only when a state changes, repeats once a day, and reports a recovery. The state lies in a table of `ops`, so a channel does not fill with repeats.
* **Delivery.** `AGORANOMOS_ALERT_URLS` in `.env`, empty by default, and then nothing goes anywhere. One or more addresses of Apprise, such as `tgram://token/chat`. Logs mask the value.
* **The content.** A code of a reason from a fixed list, the identifier of an account and a time, with no amounts and no text of a platform: anyone can write the name of a campaign, and agents may read a message.
* **The status of a backup** comes from a small shared volume, as the folder of backups has mode 700 and no access for Airflow, and [ADR-0020](0020-back-up-the-databases-every-day.md) rejected a DAG of backups, so that tasks get no passwords of the databases. The watcher sees the free disk through the volume of logs.
* **Reach.** The scheduler has a way out through the network `default`, so a message can leave in both variants. The address in `.env` reaches tasks, those of a DAG in `extensions/` too, which is the code of the deployment itself.
* **Alerts of Metabase stay for people,** for questions about the business, such as a client's spend that rose. That works today when a deployment sets up its own email or Slack. They do not become a channel of the system.
* **What it cannot do.** If the server is off or the scheduler is dead, nothing inside can report it. There is no outside check of liveness: only the silence and the dashboard say so.

The other options were not chosen for these reasons:

* **The provider of Apprise** keeps the address in a Connection, which the role Op can change: the same risk for which dlt does not read Variables. It also needs a callback on each DAG.
* **Notifiers of the image** need code and settings for each channel, and a provider of its own with `python-telegram-bot` for Telegram.
* **Alerts of Metabase** have no direct Telegram. The body of a webhook is set by code and holds rows and a picture of a chart, so data leaves; a webhook is for admins only; an alert repeats on each check; Slack needs an app of its own and email SMTP; a backup and a disk are out of its sight; and when Metabase falls, it is silent. It would, though, also work when Airflow is dead.
* **A module of our own** needs retries, rate limits, the masking of tokens and care for each channel, and the product needs the channels of all its users.
* **`on_failure_callback` of a task** does not fire for a killed task, as it runs in the process of that task. **A new service** costs memory that the budget of [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) does not have.

### Consequences

* Good, because a deployment reports a problem by itself, on the channel of its user.
* Good, because a killed task is noticed, which is how tasks fail here.
* Good, because a message holds no text from a platform, so it carries no injection.
* Good, because nothing leaves by default.
* Bad, because Apprise is a new dependency, of 9 MB, and takes 20 MiB in a process for the time of a send.
* Bad, because a server that is off stays silent, so the absence of messages proves nothing.
* Bad, because a listener in the scheduler is one more thing that runs there, and the documentation warns of that.
* Bad, because a message says little, and a person has to go to Ops or an agent for the reason.

## More Information

* **Implementation status, as of October 2026:** planned for v0.5, with the cleanup of the logs of Airflow, so that the first long-running deployment sends alerts from the start. None of it is built.
* The facts behind this record, run on 31 October 2026 on the image of the stack, Airflow 3.3.2, with one task that raises an exception, one killed by signal 9 and one that succeeds:

    | Mechanism | Exception in a task | Process of a task killed | Runs in |
    |---|---|---|---|
    | `on_failure_callback` of a task | Fired | **Did not fire** | The process of the task |
    | Listener `on_task_instance_failed` | Fired | **Did not fire** | The process of the task |
    | `on_failure_callback` of a DAG | Fired | Fired | `dag-processor` |
    | Listener `on_dag_run_failed` | Fired | Fired | The scheduler |

    * A send through Apprise 1.13.1 from `on_dag_run_failed`, in a background thread: the listener returned in 0.7 ms, a message with Cyrillic reached a local address, an unreachable address took 2 s with a timeout of `cto=2`, and the scheduler went on. The constraints of Airflow 3.3.2 hold Apprise 1.13.1 ([constraints](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt)), under BSD-2-Clause ([Apprise](https://github.com/caronc/apprise)).
    * Airflow on [listeners](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html), [callbacks](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html) and [notifications](https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html). Metabase on [webhooks](https://www.metabase.com/docs/latest/configuring-metabase/webhooks) and [alerts](https://www.metabase.com/docs/latest/questions/alerts); its body of a webhook is in `src/metabase/channel/impl/http.clj` at the tag v0.64.1.
* Not checked yet, and to be checked with the code:
    * A real send to Telegram and Slack: only a local address was used.
    * The listener on the CeleryExecutor: only the LocalExecutor was run.
* Related decisions: [ADR-0021](0021-leave-the-outgoing-traffic-of-metabase-open.md) does not change, as alerts leave from Airflow and not from Metabase. Quarantine is in [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md), and the plugins in `airflow/plugins/` follow the base package ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)).
* Revisit this record when a deployment needs an outside check of liveness, or when Airflow offers a built-in channel for every provider.
