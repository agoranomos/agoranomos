"""rates: what the loads of the sources of the rates share (ADR-0042): the order of the sources
that the setting AGORANOMOS_RATES_SOURCE gives, and when a load takes the history of the rates
again, as after a change of AGORANOMOS_REPORTING_CURRENCY.

The setting names the first source of the rates, auto by default: the National Bank of Kazakhstan
(nbk) for a reporting currency of KZT, the Bank of Italy (bank_of_italy) for another. After the
first come the other bank, for a currency that the first lacks, and the European Central Bank
(ecb); ecb alone keeps the reference rates of the ECB. The model of the rates takes them in this
order (dbt/macros/rates.sql, rates_chain), and the demo only without a rate of any of them.

core.fx_rates_daily keeps the rates of the currencies of the accounts and of the reporting currency
only, and the raw layer keeps only the loads that the core has not accepted (ADR-0035, ADR-0046).
For the days whose loads the core has accepted, a run of the transform takes the rates from the rows
that the table holds, of any reporting currency (core_fx_rates_daily): a new reporting currency
gets them if it is a currency of an account, or the reporting currency before beside a row of the
euro, and else none, and its amounts of those days are empty. The view ops.ops_rates has a row for
each source of the rates, which names the first day without a rate of the currencies of that
source and how many there are: from the first to the last day of the facts of the accounts of each
currency but the reporting currency, the days on which the currency has no rate into the reporting
currency. The source of a currency is the first in the order that the core holds a rate of it
from, and the first source of the order for a currency without one. A load of a source of the
rates reads its row, and while it names a day, the load takes the history of the rates again,
from a week before that day, or from the first day of core.fx_rates_daily when that is earlier, to
its usual last day, with the kind history in its journal (ADR-0038). The next run of the transform
pins it, reads every rate of those days, computes them in the reporting currency, and accepts and
cleans it as any other load. A load of the history fills only the days and currencies that the
core holds no rate of, so the rates that the core holds keep their source.

The load takes no history:
- for a source that the setting does not name, as with ecb alone, or for the demo once the core
  holds a rate of a real source: the model reads none of its rates then, so its DAG, unpaused or
  not, never takes its history again day after day;
- while the view does not exist, lacks these columns, as before the first run of the transform of
  this release, or was built in another reporting currency than that of the task: the next run of
  the transform, or marts-init at ./agoranomos up, builds it in the reporting currency;
- while a load of the history of the source waits for the core: whole, and not accepted, as its
  run of the transform has not ended or failed; that run computes its days, and a load of the
  history after another would bring the same.

So while a source lacks the rate of one of its currencies on some of those days, each of its loads
takes the history again, once a day, until the core holds them. A failure to read the core costs
the history and not the load: the load takes its usual window, and the next one asks again.

The view and core.fx_rates_daily are read as transformer, which writes the core and reads it, in a
transaction that only reads, and the journal of the source and core.loads as loader, through the
pipeline of the load: no role gets a right that it lacks. The module uses the standard library and
psycopg2, through agoranomos_pipelines.maintenance, and the pipeline of dlt that the load passes."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta

log = logging.getLogger(__name__)

CURRENCY = "AGORANOMOS_REPORTING_CURRENCY"
SETTING = "AGORANOMOS_RATES_SOURCE"
# The sources of the rates in their order, for each value of the setting, as rates_chain of
# dbt/macros/rates.sql gives them; auto is nbk for a reporting currency of KZT, and else
# bank_of_italy.
CHAINS = {
    "bank_of_italy": ("bank_of_italy", "nbk", "ecb"),
    "nbk": ("nbk", "bank_of_italy", "ecb"),
    "ecb": ("ecb",),
}
# The demo, which serves only a deployment without a rate of a source of the order.
DEMO = "demo_fx"
# How many days before the first day without a rate the history starts, so that its first day has
# a rate published on or before it: the longest stretch without a publication of the ECB is four
# days, from Good Friday to Easter Monday.
LEAD_DAYS = 7
# What the load reads of the core, as transformer: the rows of the view, one for each source, and
# the first day of the table.
VIEW = (
    "SELECT reporting_currency::text, rate_source, first_day_without_rate_of_source, "
    "days_without_rate_of_source FROM ops.ops_rates"
)
TABLE = "SELECT min(rate_date) FROM core.fx_rates_daily"


@dataclass(frozen=True)
class CoreRates:
    """What the core says of the rates of a source: the reporting currency of ops.ops_rates, the
    first day without a rate of the currencies of the source that it names, if any, and how many
    such days there are; and the first day of core.fx_rates_daily, if it holds a row."""

    view_currency: str
    first_without: date | None
    days_without: int
    first_day: date | None


def chain(environ: Mapping[str, str] = os.environ) -> tuple[str, ...]:
    """The sources of the rates in their order, as the setting gives them; ValueError for a value
    that is none of auto, bank_of_italy, nbk and ecb, which ./agoranomos refuses before a start."""
    setting = environ.get(SETTING) or "auto"
    if setting == "auto":
        setting = "nbk" if (environ.get(CURRENCY) or "USD") == "KZT" else "bank_of_italy"
    if setting not in CHAINS:
        raise ValueError(f"{SETTING} must be auto, bank_of_italy, nbk or ecb, not {setting!r}")
    return CHAINS[setting]


def preferred(environ: Mapping[str, str] = os.environ) -> str:
    """The first source of the rates of the setting, whose rates the model takes first."""
    return chain(environ)[0]


def transformer_password() -> str:
    """The password of transformer, which the tasks have for dbt, for mask_secret."""
    return os.environ["DBT_ENV_SECRET_WAREHOUSE_PASSWORD"]


def read_core(code: str) -> CoreRates | None:
    """What the core says of the rates of the source code, as transformer, or None while
    ops.ops_rates does not exist, lacks the columns of the days without a rate of a source, or has
    no row. A view without a row of the source names no day of it."""
    # Imported here and not at the top: the DAG processor parses the files of the DAGs often.
    from psycopg2 import errors

    from agoranomos_pipelines.maintenance import connected, transformer_credentials

    with connected(transformer_credentials(), application_name="agoranomos_rates") as connection:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            try:
                cursor.execute(VIEW)
            except (errors.UndefinedTable, errors.UndefinedColumn):
                return None
            rows = cursor.fetchall()
            if not rows:
                return None
            view_currency = rows[0][0]
            first_without, days_without = None, 0
            for _, source, first, days in rows:
                if source == code:
                    first_without, days_without = first, int(days or 0)
            cursor.execute(TABLE)
            (first_day,) = cursor.fetchone()
    return CoreRates(view_currency.strip(), first_without, days_without, first_day)


def waiting_sql(code: str, journal: str | None, loads: str | None) -> str:
    """The SQL of how many loads of the history of a source wait for the core: pinned in
    core.loads and not accepted, or whole in the journal of the source, which journal and loads
    name as dlt quotes them, and not pinned yet. A journal of a release that wrote no kind has no
    such column, so the kind of a row is read from its JSON, where it is then missing."""
    pinned = (
        "SELECT count(*) FROM core.loads "  # noqa: S608 (a code of the base package)
        f"WHERE source = '{code}' AND kind = 'history' AND status = 'pinned'"
    )
    if journal is None or loads is None:
        return pinned
    whole = (
        "SELECT count(DISTINCT j._dlt_load_id) "  # noqa: S608 (names that dlt quotes, and a code)
        f"FROM {journal} AS j JOIN {loads} AS l "
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' AND to_jsonb(j) ->> 'kind' = 'history' "
        "AND NOT EXISTS (SELECT FROM core.loads AS p "
        f"WHERE p.source = '{code}' AND p.load_id = j._dlt_load_id)"
    )
    return f"SELECT ({pinned}) + ({whole})"


def waiting(pipeline, code: str) -> int:
    """How many loads of the history of the source of a pipeline wait for the core, as loader,
    which reads its journal and core.loads (waiting_sql). A dataset without a whole load, or of a
    release before v0.5 without the journal, has none waiting in raw."""
    from dlt.destinations.exceptions import DatabaseUndefinedRelation

    from agoranomos_pipelines.loads import LOADS, has_loads

    journaled = has_loads(pipeline)
    with pipeline.sql_client() as client:
        sql = waiting_sql(code, None, None)
        if journaled:
            sql = waiting_sql(
                code,
                client.make_qualified_table_name(LOADS),
                client.make_qualified_table_name("_dlt_loads"),
            )
        try:
            rows = client.execute_sql(sql)
        except DatabaseUndefinedRelation:
            rows = client.execute_sql(waiting_sql(code, None, None))
    return int(rows[0][0]) if rows else 0


def history_from(
    pipeline,
    code: str,
    *,
    environ: Mapping[str, str] = os.environ,
    read: Callable[[str], CoreRates | None] | None = None,
) -> date | None:
    """The first day of a load of the history of the rates of the source code, whose dlt pipeline
    is pipeline, or None when the core needs none (the docstring of the module). read gives what
    the core says of a source, read_core by default."""
    currency = environ.get(CURRENCY) or "USD"
    try:
        order = chain(environ)
    except ValueError as error:
        log.warning("%s: the load takes its usual window", error)
        return None
    if code not in order and code != DEMO:
        log.info(
            "%s=%s takes the rates from %s, not from %s: the load takes its usual window and no "
            "history",
            SETTING,
            environ.get(SETTING) or "auto",
            ", ".join(order),
            code,
        )
        return None
    try:
        core = (read or read_core)(code)
    except Exception as error:  # any failure to read the core costs the history, not the load
        log.warning(
            "Could not read the rates of the core as transformer (%s): the load takes its usual "
            "window, and the next one asks again",
            type(error).__name__,
        )
        return None
    if core is None:
        log.info(
            "ops.ops_rates does not name the days without a rate yet, as before the first run of "
            "the transform of this release: the load takes its usual window"
        )
        return None
    if core.view_currency != currency:
        log.info(
            "ops.ops_rates is of %s, not of the reporting currency %s yet: the next run of the "
            "transform builds it again, and a later load takes the history if the core needs it",
            core.view_currency,
            currency,
        )
        return None
    if core.first_without is None:
        return None
    try:
        pending = waiting(pipeline, code)
    except Exception as error:  # as above: the load goes on with its usual window
        log.warning(
            "Could not read the loads of the history of %s as loader (%s): the load takes its "
            "usual window, and the next one asks again",
            code,
            type(error).__name__,
        )
        return None
    if pending:
        log.info(
            "The core lacks rates into %s on %d days from %s, and %d loads of the history of %s "
            "wait for the run of the transform that reads them: the load takes its usual window",
            currency,
            core.days_without,
            core.first_without.isoformat(),
            pending,
            code,
        )
        return None
    start = core.first_without - timedelta(days=LEAD_DAYS)
    if core.first_day is not None:
        start = min(start, core.first_day)
    log.info(
        "The core lacks rates into %s on %d days from %s, whose loads it has accepted: the load "
        "takes the history of %s again from %s (ADR-0042)",
        currency,
        core.days_without,
        core.first_without.isoformat(),
        code,
        start.isoformat(),
    )
    return start
