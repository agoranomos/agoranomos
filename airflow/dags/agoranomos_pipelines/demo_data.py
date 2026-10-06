"""Deterministic demo data: one ad account with three campaigns, one row per campaign and day.

Every number is an integer function of a BLAKE2b hash of the seed, the campaign, the day and the
field, so the rows are the same on every Python version and machine. Money is computed in whole
cents. No random module, no clock: the caller passes the last day.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal

SEED = "agoranomos-demo-v1"
ACCOUNT_ID = "demo-account-001"
CURRENCY = "USD"
DAYS = 56  # eight weeks
# campaign_id, name, impressions per day, CTR (basis points), CPC (cents), CVR (basis points)
CAMPAIGNS = (
    ("cmp-001", "Brand Search", 12_000, 600, 80, 800),
    ("cmp-002", "Prospecting Display", 90_000, 45, 45, 120),
    ("cmp-003", "Retargeting Social", 30_000, 110, 60, 300),
)
DOW_PCT = (105, 110, 108, 104, 98, 85, 90)  # Monday to Sunday


def _h(*parts: object) -> int:
    key = "|".join([SEED, *map(str, parts)]).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")


def rows(end_date: date, days: int = DAYS) -> Iterator[dict[str, object]]:
    """The rows of the days that end with end_date, oldest first."""
    start = end_date - timedelta(days=days - 1)
    for offset in range(days):
        day = start + timedelta(days=offset)
        for campaign_id, name, base, ctr_bp, cpc_cents, cvr_bp in CAMPAIGNS:
            impressions = (
                base * DOW_PCT[day.weekday()] // 100 * (95 + _h(campaign_id, day, "i") % 11) // 100
            )
            clicks = impressions * ctr_bp // 10_000 * (97 + _h(campaign_id, day, "c") % 7) // 100
            spend_cents = clicks * cpc_cents * (96 + _h(campaign_id, day, "s") % 9) // 100
            conversions_hundredths = (
                clicks * cvr_bp * (90 + _h(campaign_id, day, "v") % 21) // 10_000
            )
            yield {
                "date": day,
                "account_id": ACCOUNT_ID,
                "campaign_id": campaign_id,
                "campaign_name": name,
                "impressions": impressions,
                "clicks": clicks,
                "spend": Decimal(spend_cents) / 100,
                "conversions": Decimal(conversions_hundredths) / 100,
                "currency": CURRENCY,
            }
