"""outcome: how a run of the load of the accounts of a platform ends when some of them failed, which
the DAGs of the loads of Google Ads, of Meta, of TikTok and of GA4 share, and the short cycle of
Google Ads (ADR-0004, ADR-0035).

A load takes every account of the settings, and an error of the API on one account does not stop
the others: the load records the failed account in its journal, takes the others, and raises at the
end the failure of its connector, with one line that lists the failed accounts and their errors,
and loaded, how many accounts it took. On a try of the task that Airflow will retry, the task
raises that failure, as any error, and Airflow retries the whole load after the retry_delay of the
DAG, so that an outage of an API that clears within the retries leaves no account behind (line_of).

On the last try, the task of the load succeeds when it took at least one account: Airflow emits the
event of the asset of a task that succeeded only, so the transform takes the accounts that the load
took in its next run, and the failed ones go to quarantine as load_failed from their rows of the
journal. The task returns the line, and the last task of the DAG, check_accounts, fails with it, so
that the run ends failed and the plugin of the alerts sends its message
(airflow/plugins/agoranomos_alerts.py). A last try that took no account fails its task and emits
nothing: the transform has nothing of it to take.

The last try is the one that Airflow 3.3.2 will not retry: the API server tells the task runner to
retry a failed try when max_tries != 0 and try_number <= max_tries
(airflow/api_fastapi/execution_api/routes/task_instances.py, _is_eligible_to_retry, lines 1292 to
1301, which sets should_retry of the context of the run at line 311, and
airflow/sdk/execution_time/task_runner.py, line 1915, which reads it). The task instance of the
context gives both: try_number, from 1, and max_tries, from the context of the run (task_runner.py,
lines 249 and 1087), the retries of the task, more after a clear. With retries=0, max_tries is 0,
and the first try is the last.

check_accounts has the trigger rule none_failed and is the one leaf of its DAG, as Airflow 3.3.2
gives a run the state of its leaves alone (DagRun.update_state): after a task of the load that
failed, Airflow marks it upstream_failed, which fails the run; after one that skipped, as the short
cycle does with no account to load, it runs and passes. With all_success, a skipped load would
skip it too, and with all_done, it would run and pass after a failed load, and the run would end
green. While the load waits for a retry, check_accounts waits too.
"""

from __future__ import annotations

import logging

from airflow.sdk import task
from airflow.sdk.exceptions import AirflowFailException

log = logging.getLogger(__name__)

# The last task of each DAG of a load of accounts.
CHECK = "check_accounts"


def is_last_try(ti) -> bool:
    """Whether Airflow will not retry this try of the task instance ti when it fails: the rule of
    the API server of Airflow 3.3.2 (_is_eligible_to_retry), max_tries != 0 and try_number <=
    max_tries for a retry."""
    return ti.max_tries == 0 or ti.try_number > ti.max_tries


def line_of(failed: Exception, ti) -> str:
    """The line of the accounts that failed in a load, for the task of the load to return on its
    last try when the load took other accounts: the task succeeds, so that its asset starts the
    transform for them, and check_accounts fails the run with the line. On a try that Airflow will
    retry, the failure is raised again, so that the retry loads every account again. On the last
    try of a load that took no account, AirflowFailException with the line fails the task. failed
    is the failure of a connector, with the number of accounts that the load took in loaded; ti is
    the task instance of the context of the task."""
    if not is_last_try(ti):
        log.warning(
            "Try %d of %d: Airflow retries the load, so that the failed accounts may load too",
            ti.try_number,
            ti.max_tries + 1,
        )
        raise failed
    if not failed.loaded:
        # The line says it all: no traceback of the failure of the connector.
        raise AirflowFailException(str(failed)) from None
    log.error(
        "%s; the last try succeeds, so that the transform takes the accounts that it loaded, and "
        "%s fails the run",
        failed,
        CHECK,
    )
    return str(failed)


@task(task_id=CHECK, trigger_rule="none_failed")
def check_accounts(failures: str | None) -> None:
    """Fail the run when an account of its load failed: failures is the line that the task of the
    load returned, or None when every account loaded or the load skipped."""
    if failures:
        raise AirflowFailException(failures)
    log.info("No account of the load failed")
