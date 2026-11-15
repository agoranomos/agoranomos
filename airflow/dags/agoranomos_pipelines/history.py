"""history: the planner of the history of the ad platforms, which the loads of Google Ads and of
Meta share (ADR-0038, "The first load").

The first load of an account takes the usual window of its platform, so that a dashboard fills in
minutes, and its history comes after it, in the background, in portions: the DAG of the history of
a source runs after each run of the transform that succeeded, on the asset TRANSFORMED, and starts
at most one portion, a run of the DAG of the load of the source with the accounts and the days of
the portion as its parameters (Portion.conf). A portion is the newest calendar quarter that an
account of the source lacks days of, for every account of the source that lacks days of it
(next_portion), so that each portion goes from raw to the core whole before the next one starts,
from the new quarters to the old ones, back to a first day that the source gives: the first day of
the month 36 months before yesterday (ADR-0034), and for Meta never before the 37 months past which
the API refuses a start date (meta_ads.oldest).

The plan of a source reads, as the role loader, the windows of the whole loads of each account in
the journal _agoranomos_loads of its raw dataset and in core.loads, of every kind (known_windows),
and asks the API of the platform nothing. It starts nothing while a whole load of the source waits
for the run of the transform that pins it (waiting), before the first load of an account, whose
usual window comes first, or once every account holds its days back to the first day. Each source
plans on its own: the loads of two sources may run at the same time, as their APIs and quotas are
apart, and the one transform takes them one run at a time (max_active_runs of the transform, and
the pin of ADR-0046, which reads only the loads that have ended).
"""

from __future__ import annotations

import calendar
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta

from dlt.destinations.exceptions import DatabaseUndefinedRelation

from agoranomos_pipelines.loads import LOADED, LOADS, has_loads

log = logging.getLogger(__name__)

# The history of a real source takes the last 36 months (ADR-0034).
HISTORY_MONTHS = 36
# The code of a source goes into SQL as it is, as do the IDs of its accounts, which are digits on
# every platform that plans a history: ten on Google Ads, more on Meta.
SOURCE = re.compile(r"[a-z][a-z0-9_]*")
ACCOUNT_ID = re.compile(r"[0-9]{1,20}")


@dataclass(frozen=True)
class Portion:
    """A load of the history (ADR-0038, "The first load"): the days from first to last of some
    accounts, which a run of the DAG of the load takes from its parameters, with the kind history
    in the journal. No accounts are every account of the deployment."""

    accounts: tuple[str, ...]
    first: date
    last: date

    def conf(self) -> dict[str, object]:
        """The parameters of the run of the DAG of the load that takes the portion."""
        return {
            "accounts": list(self.accounts),
            "window_from": self.first.isoformat(),
            "window_to": self.last.isoformat(),
        }


def portion_from(
    account_ids: Iterable[str] | None,
    first: str | None,
    last: str | None,
    *,
    form: re.Pattern,
    shape: str,
    error: type[Exception],
) -> Portion | None:
    """The portion that the parameters of a run of the DAG of a load name: accounts, each of the
    form of its platform, which shape says in words, or none for every account, and the first and
    last day, as ISO dates; none when the run names no days, as a scheduled run does, which takes
    the usual window of each account. A wrong parameter raises error, that of the platform."""
    account_ids = list(account_ids or ())
    if first is None and last is None:
        if account_ids:
            raise error(
                "a run of some accounts names the days of their history too: window_from and "
                "window_to"
            )
        return None
    if first is None or last is None:
        raise error("a run of the history names both window_from and window_to")
    try:
        days = date.fromisoformat(first), date.fromisoformat(last)
    except (TypeError, ValueError):
        raise error("window_from and window_to are dates, such as 2026-07-01") from None
    if days[0] > days[1]:
        raise error(f"window_from {first} is after window_to {last}")
    wrong = [account_id for account_id in account_ids if not form.fullmatch(account_id)]
    if wrong:
        raise error(f"an account of a run is {shape}, not {wrong[0]!r}")
    return Portion(tuple(sorted(set(account_ids))), *days)


def months_back(day: date, months: int) -> date:
    """The same day of the month months before day, or the last day of that month when it is
    shorter."""
    index = day.year * 12 + day.month - 1 - months
    year, month = index // 12, index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def history_start(last: date, months: int = HISTORY_MONTHS) -> date:
    """The first day of the history of an account whose last day is last: the first day of the
    month months before it."""
    return months_back(last, months).replace(day=1)


def quarter(day: date) -> tuple[date, date]:
    """The first and the last day of the calendar quarter of a day."""
    first = date(day.year, (day.month - 1) // 3 * 3 + 1, 1)
    following = date(first.year + first.month // 10, (first.month + 2) % 12 + 1, 1)
    return first, following - timedelta(days=1)


def missing(windows: Iterable[tuple[date, date]], limit: date) -> list[tuple[date, date]]:
    """The days of the history of an account that no window of its whole loads holds, from limit
    to the last day that they reach, as runs of days from the oldest; none before its first load,
    whose usual window comes first."""
    held = sorted(windows)
    if not held:
        return []
    gaps, start = [], limit
    for first, last in held:
        if first > start:
            gaps.append((start, first - timedelta(days=1)))
        start = max(start, last + timedelta(days=1))
    return gaps


def next_portion(known: dict[str, list[tuple[date, date]]], limit: date) -> Portion | None:
    """The next load of the history, from the windows of the whole loads of each account, known:
    the newest calendar quarter that an account lacks days of, back to limit, for every account
    that lacks days of it, from the first to the last of those days, so that a portion takes one
    quarter across all the new accounts and the history goes from the new quarters to the old
    ones (ADR-0038). An account whose days of the quarter are fewer takes the others again, which
    the newest load replaces whole. None once no account lacks a day."""
    lacking = {account_id: missing(windows, limit) for account_id, windows in known.items()}
    ends = [gap[1] for gaps in lacking.values() for gap in gaps]
    if not ends:
        return None
    start, end = quarter(max(ends))
    taken = {
        account_id: [(max(first, start), min(last, end)) for first, last in gaps]
        for account_id, gaps in lacking.items()
    }
    taken = {
        account_id: [(first, last) for first, last in days if first <= last]
        for account_id, days in taken.items()
    }
    taken = {account_id: days for account_id, days in taken.items() if days}
    return Portion(
        tuple(sorted(taken)),
        min(first for days in taken.values() for first, _ in days),
        max(last for days in taken.values() for _, last in days),
    )


def source_checked(source: str) -> str:
    """The code of a source, which goes into SQL as it is, so nothing but lower case may."""
    if not SOURCE.fullmatch(source):
        raise ValueError(f"the code of a source is lower case letters, not {source!r}")
    return source


def checked(source: str, account_ids: Iterable[str]) -> list[str]:
    """The IDs of the accounts of a source, sorted, after its code: at least one, each digits. They
    go into SQL as they are, so nothing else may."""
    source_checked(source)
    found = sorted(account_ids)
    if not found or not all(ACCOUNT_ID.fullmatch(account_id) for account_id in found):
        raise ValueError("the history of a source takes one account ID of digits or more")
    return found


def known_windows(
    pipeline, source: str, account_ids: Iterable[str]
) -> dict[str, list[tuple[date, date]]]:
    """The windows of the whole loads of each account of a source, those that _dlt_loads marks
    whole in the journal of the dataset of pipeline and those that core.loads holds, which loader
    may read, of every kind; an empty list for an account that no whole load took yet."""
    wanted = checked(source, account_ids)
    found: dict[str, list[tuple[date, date]]] = {account_id: [] for account_id in wanted}
    # IDs of checked digits, and a checked code.
    listed = ", ".join(f"'{account_id}'" for account_id in wanted)
    queries = [
        "SELECT account_id, window_from, window_to FROM core.loads "  # noqa: S608
        f"WHERE source = '{source}' AND outcome = '{LOADED}' AND account_id IN ({listed})"
    ]
    journaled = has_loads(pipeline)
    with pipeline.sql_client() as client:
        if journaled:
            journal = client.make_qualified_table_name(LOADS)
            loads = client.make_qualified_table_name("_dlt_loads")
            # Names that dlt quotes, IDs of checked digits, and constants of this module.
            queries.append(
                "SELECT j.account_id, j.window_from, j.window_to "  # noqa: S608
                f"FROM {journal} AS j JOIN {loads} AS l "
                "ON l.load_id = j._dlt_load_id AND l.status = 0 "
                f"WHERE j.status = '{LOADED}' AND j.account_id IN ({listed})"
            )
        for sql in queries:
            try:
                rows = client.execute_sql(sql)
            except DatabaseUndefinedRelation:
                continue
            for account_id, first, last in rows or ():
                if first is not None and last is not None:
                    found[account_id].append((first, last))
    return found


def waiting(pipeline, source: str) -> int:
    """How many whole loads of the accounts of the journal of a source no run of the transform has
    pinned yet, as core.loads holds none of their rows: the next portion of the history waits for
    the run of the transform that reads them."""
    source_checked(source)
    if not has_loads(pipeline):
        return 0
    try:
        with pipeline.sql_client() as client:
            journal = client.make_qualified_table_name(LOADS)
            loads = client.make_qualified_table_name("_dlt_loads")
            # Names that dlt quotes, a checked code, and constants of this module.
            rows = client.execute_sql(
                "SELECT count(DISTINCT j._dlt_load_id) "  # noqa: S608
                f"FROM {journal} AS j JOIN {loads} AS l "
                "ON l.load_id = j._dlt_load_id AND l.status = 0 "
                "WHERE NOT EXISTS (SELECT FROM core.loads AS p "
                f"WHERE p.source = '{source}' AND p.load_id = j._dlt_load_id "
                "AND p.account_id = j.account_id)"
            )
    except DatabaseUndefinedRelation:
        # A dataset of a release before v0.5, without the journal: no load of it is pinned.
        return 0
    return int(rows[0][0]) if rows else 0


def planned(
    pipeline, source: str, platform: str, account_ids: Iterable[str], limit: date
) -> Portion | None:
    """The next portion of the history of the accounts of a source, back to limit, from the
    dataset of pipeline, or none: when every account holds its history, when no account has had
    its usual window yet, or while a whole load of the source waits for the run of the transform
    that pins it, so that each portion goes from raw to the core before the next one starts
    (ADR-0038). platform names the source in the log."""
    wanted = checked(source, account_ids)
    pending = waiting(pipeline, source)
    if pending:
        log.info(
            "%d whole loads of %s wait for the run of the transform that pins them: the history "
            "waits for it",
            pending,
            platform,
        )
        return None
    portion = next_portion(known_windows(pipeline, source, wanted), limit)
    if portion is None:
        log.info(
            "Every account of %s that has had its usual window holds its days back to %s",
            platform,
            limit.isoformat(),
        )
    else:
        log.info(
            "The next portion of the history: %s to %s of %d accounts (%s) of %s",
            portion.first.isoformat(),
            portion.last.isoformat(),
            len(portion.accounts),
            ", ".join(portion.accounts),
            platform,
        )
    return portion
