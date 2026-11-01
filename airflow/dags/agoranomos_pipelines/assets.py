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

# The asset of the rates of the ECB (ADR-0032), in the same form, which each of its loads updates.
RAW_ECB = Asset("postgres://warehouse-db:5432/warehouse/raw_ecb/rates_daily")

# The asset of the statistics of Google Ads (ADR-0032), in the same form, which each of its loads
# updates.
RAW_GOOGLE_ADS = Asset("postgres://warehouse-db:5432/warehouse/raw_google_ads/campaign_stats")

# The condition on which the transform runs (ADR-0039): the demo has loaded, or the rates have, or
# Google Ads has. A source that joins the model adds its asset here. The transform allows one run
# at a time: while a run is queued or running, Airflow keeps the events that come, and starts one
# run for all of them once it ends.
LOADED = AssetAny(DEMO_LOADED, RAW_ECB, RAW_GOOGLE_ADS)
