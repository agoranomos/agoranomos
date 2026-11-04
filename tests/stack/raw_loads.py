"""The rows of a fact table of the demo in raw that staging reads, as SQL that does not use the
macro of the model: for each account and day, those of the newest whole load whose window in the
journal _agoranomos_loads covers the day (dbt/macros/newest_load.sql, ADR-0038). Each load of the
demo appends its rows under its own _dlt_load_id, so a table holds every load until the core
accepts it, and the tests of the demo compare the rows that this rule reads, not the table."""

from __future__ import annotations

from agoranomos_pipelines.demo import platforms

# The newest whole load of each account and day of a dataset, {dataset} in it: only the loads that
# dlt finished, with their row in _dlt_loads and status 0, only the accounts that loaded, and the
# largest load id, a time in seconds as text, compared as a number.
DAYS = (
    "SELECT DISTINCT ON (j.account_id, w.day) j.account_id, w.day, j._dlt_load_id AS load_id "
    "FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "CROSS JOIN LATERAL (SELECT j.window_from + n AS day "
    "FROM generate_series(0, j.window_to - j.window_from) AS n) AS w "
    "WHERE j.status = 'loaded' "
    "ORDER BY j.account_id, w.day, j._dlt_load_id::numeric DESC"
)


def account_of(fact: platforms.FactTable) -> str | None:
    """The account of a row of a fact table as the journal names it, as SQL, or None for the
    rates, whose one account is the base of the rates: properties/{} of GA4 is the ID after the
    slash."""
    if fact.account_sql is None:
        return None
    if fact.account_text == "{}":
        return fact.account_sql
    return f"split_part({fact.account_sql}, '/', 2)"


def newest(code: str, table: str | None = None) -> str:
    """The rows of a fact table of a platform of the demo, its main table by default, that staging
    reads, as a subquery with the alias newest: the columns of the table, and _account and _day,
    the account and the day of the row."""
    platform = platforms.PLATFORMS[code]
    [fact] = [f for f in platform.facts if f.table == (table or platform.main_table)]
    account = account_of(fact)
    rows = (
        f"SELECT *, {account or 'NULL'} AS _account, ({fact.day_date_sql})::date AS _day "  # noqa: S608 (constants of the demo)
        f"FROM {platform.dataset}.{fact.table}"
    )
    matched = "d.load_id = r._dlt_load_id AND d.day = r._day"
    if account is not None:
        matched += " AND d.account_id = r._account"
    days = DAYS.format(dataset=platform.dataset)
    return f"(SELECT r.* FROM ({rows}) AS r JOIN ({days}) AS d ON {matched}) AS newest"  # noqa: S608 (constants of the demo)
