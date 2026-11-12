"""acceptance: the end of a run of the transform whose tasks of dbt all succeeded (ADR-0035,
ADR-0046): the core accepts the loads that the run read, and then the raw layer loses their rows,
each step safe to repeat, in this order:

1. accept, as transformer: each row of core.loads that this run or an earlier one pinned becomes
   accepted, by load and account, but those of an account that the checks of this run held in
   quarantine, a stopped source among them. A load pinned after the pin of the run stays pinned,
   as no run has read it. In the same transaction, the days of the accepted loads leave
   staging.int_window_days, the days that the models of the facts replace: so dbt by hand after the
   run writes no day of a load whose rows the cleanup deletes, which would empty the day, and the
   tests of the windows by hand find none to compare.
2. clean_raw, as loader, the owner of the raw schemas: in each table of a source, the rows of the
   accepted loads go, by load and account, as one load can be accepted for some accounts and wait
   in quarantine for others, and so do the rows of a load that did not end and is older than
   UNFINISHED_AFTER. Under LOCK TABLE ... IN ACCESS EXCLUSIVE MODE, a table that holds no other row
   is emptied by TRUNCATE, in the transaction of the check, as a check apart from it let a load in
   progress write between the two (#188); a table that holds another row, or whose lock does not
   come within LOCK_TIMEOUT, loses the rows by DELETE instead.

The journal of the loads of each source, _agoranomos_loads, and the tables of dlt stay whole: the
next load of the demo and of the rates starts its window from the journal (loads.recorded), the pin
reads the journal and _dlt_loads, dlt restores its state from _dlt_pipeline_state, and the DAG of
maintenance cleans _dlt_loads by its own rules (maintenance.py).

A step that fails leaves nothing half done: the acceptance is one statement, and the cleanup of
each table one transaction, and a row that is accepted and not deleted yet is read by no run, as
staging reads the rows of the pinned loads only (dbt/macros/newest_load.sql).
"""

from __future__ import annotations

import logging
import re
import time
from datetime import timedelta

from agoranomos_pipelines.maintenance import (
    connected,
    loader_credentials,
    seconds_since,
    transformer_credentials,
)

log = logging.getLogger(__name__)

# The tables of this run that tell what it read and what it held: the newest pinned load of each
# account and day, which the run filled from core.loads after its pin, and the checks of G0a, G1
# and G1a, one row for each load, account and reason that failed, a stopped source among them
# (dbt/models/staging/loads/stg_loads__newest_days.sql and
# dbt/models/intermediate/int_g1_failures.sql).
NEWEST_DAYS = "staging.stg_loads__newest_days"
FAILURES = "staging.int_g1_failures"
# The days of the windows of the run that the models of the facts replace, by account and day,
# with the surrogate of the load of each (dbt/models/intermediate/int_window_days.sql).
WINDOW_DAYS = "staging.int_window_days"

# The rows of a load that has no row in _dlt_loads and none in core.loads, a load that stopped half
# way, go once its load id, the moment when dlt began to extract it, is older than this: longer than
# the dagrun_timeout of every DAG that loads, seven hours for Google Ads and an hour for the others,
# so that no load that is still running loses a row (ADR-0046). tests/unit/test_acceptance.py
# compares it with the DAGs.
UNFINISHED_AFTER = timedelta(days=1)

# How long the cleanup waits for the lock of a table before it deletes instead: a load in progress
# holds a lower lock while it writes a file, and a write that comes while the cleanup waits waits
# behind it, so the wait stays short.
LOCK_TIMEOUT = "10s"

# The tables of the raw layer that the cleanup leaves alone: those of dlt and the journals of the
# loads of agoranomos.
KEPT_PREFIXES = ("_dlt_", "_agoranomos_")

# How the rows of each table of a source in raw name the account of their load, as SQL over the
# alias r, as the staging models read them: for a fact table, the account of its source in
# newest_load_sources of dbt/macros/newest_load.sql, and for a list, the account that its model
# passes to newest_list, if any. None for the rates, whose loads have one account each, the base
# of the rates. A table that is not here, or whose account is None, loses its rows only by whole
# load, once every account of the load is accepted. tests/unit/test_acceptance.py compares this with
# the staging models.
SEARCH = "r.customer__id"
SOCIAL = "r.account_id"
VIDEO = "r.advertiser_id"
ACCOUNTS: dict[str, dict[str, str | None]] = {
    "raw_demo_fx": {"rates_daily": None},
    "raw_demo_search": {
        "customer_clients": "r.customer_client__id",
        "campaigns": SEARCH,
        "ad_groups": SEARCH,
        "ads": SEARCH,
        "campaign_stats": SEARCH,
        "campaign_conversions": SEARCH,
        "ad_group_stats": SEARCH,
        "ad_group_conversions": SEARCH,
        "ad_stats": SEARCH,
        "ad_conversions": SEARCH,
    },
    "raw_demo_social": {
        "ad_accounts": SOCIAL,
        "campaigns": SOCIAL,
        "adsets": SOCIAL,
        "ads": SOCIAL,
        "insights_campaign": SOCIAL,
        "insights_adset": SOCIAL,
        "insights_ad": SOCIAL,
    },
    "raw_demo_video": {
        "advertisers": VIDEO,
        "campaigns": VIDEO,
        "adgroups": VIDEO,
        "ads": VIDEO,
        "report_campaign_daily": VIDEO,
        "report_adgroup_daily": VIDEO,
        "report_ad_daily": VIDEO,
    },
    "raw_demo_web": {
        "properties": "split_part(r.name, '/', 2)",
        "report_sessions_daily": "split_part(r.property, '/', 2)",
    },
    "raw_ecb": {"rates_daily": None},
    "raw_google_ads": {
        "customers": SEARCH,
        "campaigns": SEARCH,
        "ad_groups": SEARCH,
        "ads": SEARCH,
        "conversion_actions": SEARCH,
        "customer_stats": SEARCH,
        "campaign_stats": SEARCH,
        "campaign_conversions": SEARCH,
        "ad_group_stats": SEARCH,
        "ad_group_conversions": SEARCH,
        "ad_stats": SEARCH,
        "ad_conversions": SEARCH,
    },
}

# The acceptance (ADR-0046). The bound is the newest pin that this run read: the latest pinned_at
# of the loads of the days of the run. Pins go one after another, so every row pinned at or before
# that moment was there when the run read core.loads, also one that no day of the run takes, as a
# load that a newer one covers whole, or an account that failed in its load; a row that a pin wrote
# later, which only dbt by hand with the ID of a run can do, waits for the next run. An account
# that the checks of this run held is held with every load that it has pinned: the core did not
# take its days, and an older load of it may hold days that the newer one does not. A run that read
# no day accepts nothing.
ACCEPT = f"""
WITH read AS (
    SELECT max(l.pinned_at) AS pinned_at
    FROM core.loads AS l
    JOIN {NEWEST_DAYS} AS d
        ON d.source = l.source AND d.load_id = l.load_id AND d.account_id = l.account_id
)
UPDATE core.loads AS l
SET status = 'accepted', accepted_at = now()
FROM read
WHERE l.status = 'pinned'
    AND l.pinned_at <= read.pinned_at
    AND NOT EXISTS (
        SELECT FROM {FAILURES} AS q
        WHERE q.source = l.source AND q.account_id = l.account_id
    )
RETURNING l.source
"""  # noqa: S608 (constants of this module)
# Then the days of the accepted loads leave the days of the windows. The model of the days reads
# the pinned loads only, so a build of it by hand after the run gives none of them either.
FORGET_DAYS = f"""
DELETE FROM {WINDOW_DAYS} AS w
USING core.loads AS l
WHERE l.load_sk = w.load_sk AND l.status = 'accepted'
"""  # noqa: S608 (constants of this module)


def accept() -> dict[str, int]:
    """Mark as accepted, as transformer, what this run read and kept, by load and account, take
    their days out of the days of the windows, in one transaction, and return the rows of
    core.loads that each source got accepted."""
    started = time.monotonic()
    with (
        connected(transformer_credentials()) as connection,
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT to_regclass(%s) IS NOT NULL AND to_regclass(%s) IS NOT NULL"
            " AND to_regclass(%s) IS NOT NULL",
            (NEWEST_DAYS, FAILURES, WINDOW_DAYS),
        )
        if not cursor.fetchone()[0]:
            # Only before the first run of the transform has built them: nothing was read.
            log.warning("A table of the run does not exist yet: nothing accepted")
            return {}
        cursor.execute(ACCEPT)
        accepted: dict[str, int] = {}
        for (source,) in cursor.fetchall():
            accepted[source] = accepted.get(source, 0) + 1
        cursor.execute(FORGET_DAYS)
        forgotten = cursor.rowcount
    for source, count in sorted(accepted.items()):
        log.info("Accepted %d rows of core.loads of %s", count, source)
    log.info(
        "Accepted %d rows of core.loads, and took %d days out of %s, in %.1f seconds",
        sum(accepted.values()),
        forgotten,
        WINDOW_DAYS,
        seconds_since(started),
    )
    return accepted


def quoted(name: str) -> str:
    """A name of the catalog that dlt gives a schema or a table, in quotes; any other fails."""
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError(f"{name} is not a name that dlt gives")
    return f'"{name}"'


def removable(schema: str, account: str | None) -> str:
    """Whether a row r of a table of the raw schema schema goes, as SQL with the parameters source
    and cut: its load is accepted for the account of the row; or the row names no account of its
    load in core.loads, as the row of the manager in the list of the accounts of Google Ads, or
    the table names no account (account None), and every account of the load is accepted; or its
    load did not end and is older than the cut, with no row in _dlt_loads and none in core.loads.
    Each list is one hashed lookup of PostgreSQL, so the condition reads each table once. A load id
    that is no number, as a row that a person wrote can have, is no load that did not end."""
    whole = (
        "r._dlt_load_id IN (SELECT load_id FROM core.loads WHERE source = %(source)s"
        " GROUP BY load_id HAVING bool_and(status = 'accepted'))"
    )
    if account is None:
        accepted = whole
    else:
        named = f"(r._dlt_load_id, ({account})::text)"
        accepted = (
            f"{named} IN (SELECT load_id, account_id FROM core.loads"  # noqa: S608 (SQL of this module)
            " WHERE source = %(source)s AND status = 'accepted')"
            f" OR (({account}) IS NULL OR {named} NOT IN (SELECT load_id, account_id"
            f" FROM core.loads WHERE source = %(source)s)) AND {whole}"
        )
    unfinished = (
        f"r._dlt_load_id NOT IN (SELECT load_id FROM {quoted(schema)}._dlt_loads"  # noqa: S608
        " WHERE load_id IS NOT NULL)"
        " AND r._dlt_load_id NOT IN (SELECT load_id FROM core.loads WHERE source = %(source)s)"
        r" AND CASE WHEN r._dlt_load_id ~ '^[0-9]+(\.[0-9]+)?$'"
        " THEN to_timestamp(r._dlt_load_id::double precision) END"
        " < now() - %(cut)s::interval"
    )
    return f"(({accepted}) OR ({unfinished}))"


def children_of(table: str, roots: list[str], nested: list[str]) -> list[str]:
    """The child tables of a root table, which dlt names <root>__<field> and joins to their row by
    _dlt_parent_id, the _dlt_id of the row: those whose longest root prefix is this table. A child
    of a child, whose parent is another child table, is none: its rows join no row of the root, so
    the cleanup leaves it as it is (no such table exists today)."""
    found = []
    for child in nested:
        parents = [root for root in roots if child.startswith(f"{root}__")]
        nested_in = [other for other in nested if child.startswith(f"{other}__")]
        if parents and max(parents, key=len) == table and not nested_in:
            found.append(child)
    return sorted(found)


def tables_of(cursor, schema: str) -> tuple[list[str], list[str]]:
    """The root tables of a raw schema, with _dlt_load_id, and its child tables, with
    _dlt_parent_id and no _dlt_load_id, but those of dlt and the journals."""
    cursor.execute(
        "SELECT table_name, bool_or(column_name = '_dlt_load_id'),"
        " bool_or(column_name = '_dlt_parent_id')"
        " FROM information_schema.columns WHERE table_schema = %s"
        " AND column_name IN ('_dlt_load_id', '_dlt_parent_id') GROUP BY table_name ORDER BY 1",
        (schema,),
    )
    roots, nested = [], []
    for table, has_load, has_parent in cursor.fetchall():
        if table.startswith(KEPT_PREFIXES):
            continue
        if has_load:
            roots.append(table)
        elif has_parent:
            nested.append(table)
    return roots, nested


def raw_schemas(cursor) -> list[str]:
    """The raw schemas of loader whose loads the pin takes: those with the journal of the loads and
    _dlt_loads."""
    cursor.execute(
        "SELECT n.nspname FROM pg_namespace AS n"
        " WHERE n.nspname LIKE 'raw\\_%' AND pg_get_userbyid(n.nspowner) = current_user"
        " AND EXISTS (SELECT FROM pg_class AS c WHERE c.relnamespace = n.oid"
        " AND c.relname = '_agoranomos_loads')"
        " AND EXISTS (SELECT FROM pg_class AS c WHERE c.relnamespace = n.oid"
        " AND c.relname = '_dlt_loads')"
        " ORDER BY 1"
    )
    return [row[0] for row in cursor.fetchall()]


def truncate_if_all_go(
    cursor, schema: str, table: str, children: list[str], condition: str, parameters: dict
) -> int | None:
    """In the transaction of the cursor, under the lock of the table and of its child tables: the
    rows that go when no other row is left, and TRUNCATE of them all; None when a row stays, a child
    row has no row of the table, as a load in progress may write the child first, or the table is
    empty. Raises the error of the lock when it does not come within LOCK_TIMEOUT."""
    names = [f"{quoted(schema)}.{quoted(name)}" for name in (table, *children)]
    cursor.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    cursor.execute(f"LOCK TABLE {', '.join(names)} IN ACCESS EXCLUSIVE MODE")
    cursor.execute(
        f"SELECT count(*), count(*) FILTER (WHERE NOT coalesce({condition}, false))"  # noqa: S608
        f" FROM {names[0]} AS r",
        parameters,
    )
    rows, staying = cursor.fetchone()
    if rows == 0 or staying:
        return None
    gone = rows
    for child in names[1:]:
        cursor.execute(
            f"SELECT count(*), count(*) FILTER (WHERE r._dlt_id IS NULL) FROM {child} AS c"  # noqa: S608
            f" LEFT JOIN {names[0]} AS r ON r._dlt_id = c._dlt_parent_id"
        )
        child_rows, orphans = cursor.fetchone()
        if orphans:
            return None
        gone += child_rows
    cursor.execute(f"TRUNCATE {', '.join(names)}")
    return gone


def delete_what_goes(
    cursor, schema: str, table: str, children: list[str], condition: str, parameters: dict
) -> int:
    """In the transaction of the cursor: the rows of the child tables whose row goes, then the rows
    of the table that go, by DELETE, and how many rows went."""
    parent = f"{quoted(schema)}.{quoted(table)}"
    gone = 0
    for child in children:
        cursor.execute(
            f"DELETE FROM {quoted(schema)}.{quoted(child)} AS c USING {parent} AS r"  # noqa: S608
            f" WHERE c._dlt_parent_id = r._dlt_id AND {condition}",
            parameters,
        )
        gone += cursor.rowcount
    cursor.execute(f"DELETE FROM {parent} AS r WHERE {condition}", parameters)  # noqa: S608
    return gone + cursor.rowcount


def clean_table(
    connection, schema: str, table: str, children: list[str], cut: timedelta
) -> tuple[str, int]:
    """Clean one table of raw and its child tables, each way in a transaction of its own: TRUNCATE
    when nothing else is left and the lock comes, and else DELETE. Returns the way and the rows that
    went."""
    # Imported here and not at the top: the DAG processor parses the file of the DAG often.
    from psycopg2 import errors

    condition = removable(schema, ACCOUNTS.get(schema, {}).get(table))
    parameters = {"source": schema.removeprefix("raw_"), "cut": f"{cut.total_seconds()} seconds"}
    try:
        with connection, connection.cursor() as cursor:
            gone = truncate_if_all_go(cursor, schema, table, children, condition, parameters)
        if gone is not None:
            return "truncate", gone
    except errors.LockNotAvailable:
        log.info("%s.%s is in use: its rows go by DELETE", schema, table)
    with connection, connection.cursor() as cursor:
        return "delete", delete_what_goes(cursor, schema, table, children, condition, parameters)


def clean_raw(cut: timedelta = UNFINISHED_AFTER) -> dict[str, int]:
    """Delete, as loader, the rows of the accepted loads and of the loads that did not end and are
    older than cut from each table of each raw schema whose loads the pin takes, and return how
    many rows each schema lost."""
    started = time.monotonic()
    cleaned: dict[str, int] = {}
    with connected(loader_credentials()) as connection:
        with connection, connection.cursor() as cursor:
            schemas = raw_schemas(cursor)
        for schema in schemas:
            with connection, connection.cursor() as cursor:
                roots, nested = tables_of(cursor, schema)
            placed = set()
            cleaned[schema] = 0
            for table in roots:
                children = children_of(table, roots, nested)
                placed.update(children)
                table_started = time.monotonic()
                way, gone = clean_table(connection, schema, table, children, cut)
                cleaned[schema] += gone
                log.info(
                    "%s.%s%s: %d rows went by %s in %.1f seconds",
                    schema,
                    table,
                    f" with {', '.join(children)}" if children else "",
                    gone,
                    way.upper(),
                    seconds_since(table_started),
                )
            for child in sorted(set(nested) - placed):
                log.warning("%s.%s has no table of its rows here: it stays as it is", schema, child)
    log.info(
        "The raw layer lost %d rows in %.1f seconds", sum(cleaned.values()), seconds_since(started)
    )
    return cleaned
