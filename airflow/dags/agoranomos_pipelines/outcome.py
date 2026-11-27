"""outcome: how a run of the load of the accounts of a platform ends when some of them failed, which
the DAGs of the loads of Google Ads, of Meta, of TikTok and of GA4 share, and the short cycle of
Google Ads (ADR-0004, ADR-0035).

A load takes every account of the settings, and an error of the API on one account does not stop
the others: the load records the failed account in its journal, takes the others, and raises at the
end the failure of its connector, with one line that lists the failed accounts and their errors,
and loaded, how many accounts it took. The task of the load then succeeds when it took at least one
account (line_of): Airflow emits the event of the asset of a task that succeeded only, so the
transform takes the accounts that the load took in its next run, and the failed ones go to
quarantine as load_failed from their rows of the journal. The task returns the line, and the last
task of the DAG, check_accounts, fails with it, so that the run ends failed and the plugin of the
alerts sends its message (airflow/plugins/agoranomos_alerts.py). A load that took no account fails
its task, without a retry, and emits nothing: the transform has nothing of it to take.

check_accounts has the trigger rule none_failed and is the one leaf of its DAG, as Airflow 3.3.2
gives a run the state of its leaves alone (DagRun.update_state): after a task of the load that
failed, Airflow marks it upstream_failed, which fails the run; after one that skipped, as the short
cycle does with no account to load, it runs and passes. With all_success, a skipped load would
skip it too, and with all_done, it would run and pass after a failed load, and the run would end
green.
"""

from __future__ import annotations

import logging

from airflow.sdk import task
from airflow.sdk.exceptions import AirflowFailException

log = logging.getLogger(__name__)

# The last task of each DAG of a load of accounts.
CHECK = "check_accounts"


def line_of(failed: Exception) -> str:
    """The line of the accounts that failed in a load that took others, for the task of the load
    to return: it succeeds, so that its asset starts the transform for the accounts that it took,
    and check_accounts fails the run with the line. A load that took no account raises
    AirflowFailException with the line instead, which fails its task without a retry: the next
    load takes the days that the failed accounts missed. failed is the failure of a connector, with
    the number of accounts that the load took in loaded."""
    if not failed.loaded:
        # The line says it all: no traceback of the failure of the connector.
        raise AirflowFailException(str(failed)) from None
    log.error(
        "%s; the task succeeds, so that the transform takes the accounts that it loaded, and %s "
        "fails the run",
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
