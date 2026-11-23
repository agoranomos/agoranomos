"""The assets that the DAGs of the agoranomos bundle share.

A DAG file takes an asset from here and never from another DAG file, which would define the DAGs of
that file twice.
"""

from airflow.sdk import Asset, AssetAll, AssetAny

from agoranomos_pipelines.demo.platforms import PLATFORMS, asset_uri

# The assets of the main tables of the five fictional platforms of the demo, in the form that
# Airflow 3 requires for PostgreSQL: postgres://<host>:<port>/<database>/<schema>/<table>. Each
# load of a platform updates its own. The dbt profile uses the same host, port and database, so the
# assets that Cosmos emits for the models have the same form.
RAW_DEMO_PLATFORMS = {code: Asset(asset_uri(platform)) for code, platform in PLATFORMS.items()}

# The five platforms of the demo as one condition: each has loaded since the last run of the
# transform. The history and the daily load take them one after another, in one run of their DAG,
# so one load of the demo starts one run of the transform, after its last platform. As five
# conditions of their own, Airflow 3.3.2 would start a run after the first platform, keep the
# events of the other four while it runs, as the DAG allows one run at a time, and start one more
# run for them when it ends: two runs for one load, the first reading tables whose days the loads
# still replace, which nothing prevents until the pinning of ADR-0038. The pinning is what lets
# the five become conditions of their own (ADR-0039).
DEMO_LOADED = AssetAll(*RAW_DEMO_PLATFORMS.values())

# The asset of the current day of the demo (ADR-0051), which each load of its short cycle updates:
# a load of the kind intraday into the main table of demo_search, under an asset of its own, which
# starts a run of the transform alone, as no load of the other platforms comes with it. An address
# of PostgreSQL names one asset, that of demo_search, so this one has a name and no address.
RAW_DEMO_INTRADAY = Asset("agoranomos_demo_intraday")

# The asset of the rates of the ECB (ADR-0032), in the same form, which each of its loads updates.
RAW_ECB = Asset("postgres://warehouse-db:5432/warehouse/raw_ecb/rates_daily")

# The asset of the rates of the Bank of Italy (ADR-0042), in the same form, which each of its loads
# updates.
RAW_BANK_OF_ITALY = Asset("postgres://warehouse-db:5432/warehouse/raw_bank_of_italy/rates_daily")

# The asset of the statistics of Google Ads (ADR-0032), in the same form, which each of its loads
# updates.
RAW_GOOGLE_ADS = Asset("postgres://warehouse-db:5432/warehouse/raw_google_ads/campaign_stats")

# The asset of the current day of Google Ads (ADR-0051), which each load of the short cycle
# updates: a load of the kind intraday into the same tables, under an asset of its own, so that the
# asset state store of RAW_GOOGLE_ADS keeps the watermarks of the daily load alone. Both are loads
# of campaign_stats, and an address of PostgreSQL names one asset, so this one has a name and no
# address.
RAW_GOOGLE_ADS_INTRADAY = Asset("agoranomos_google_ads_intraday")

# The asset of the insights of Meta (ADR-0050), in the same form, which each of its loads updates.
RAW_META_ADS = Asset("postgres://warehouse-db:5432/warehouse/raw_meta_ads/insights_campaign")

# The asset of the report of GA4 (ADR-0054), in the same form, which each of its loads updates.
RAW_GA4 = Asset("postgres://warehouse-db:5432/warehouse/raw_ga4/report_sessions_daily")

# The asset of the reports of TikTok (ADR-0053), in the same form, which each of its loads updates.
RAW_TIKTOK_ADS = Asset(
    "postgres://warehouse-db:5432/warehouse/raw_tiktok_ads/report_campaign_daily"
)

# The asset of the official rates of the National Bank of Kazakhstan (ADR-0042), in the same form,
# which each of its loads updates.
RAW_NBK = Asset("postgres://warehouse-db:5432/warehouse/raw_nbk/rates_daily")

# The asset of a run of the transform in which every task succeeded, which the last task of the
# transform updates: the histories of Google Ads, of Meta and of TikTok plan their next portions
# after it, so that each portion goes from raw to the core before the next one starts (ADR-0038,
# "The first load"). It is no table, so it has a name and no address of PostgreSQL.
TRANSFORMED = Asset("agoranomos_transformed")

# The condition on which the transform runs (ADR-0039): the demo has loaded, or the rates of the ECB
# or of the Bank of Italy have, or Google Ads, Meta or GA4 has, or the rates of the NBK have, or
# TikTok has, or the short cycle of Google Ads or of the demo has (ADR-0051); no run reads the loads
# of GA4 until its staging (C6c of #226). A source that joins the model adds its asset here. The
# transform allows one run at a time: while a run is queued or running, Airflow keeps the events
# that come, and starts one run for all of them once it ends.
LOADED = AssetAny(
    DEMO_LOADED,
    RAW_ECB,
    RAW_BANK_OF_ITALY,
    RAW_GOOGLE_ADS,
    RAW_META_ADS,
    RAW_GA4,
    RAW_NBK,
    RAW_TIKTOK_ADS,
    RAW_GOOGLE_ADS_INTRADAY,
    RAW_DEMO_INTRADAY,
)
