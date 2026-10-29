"""The assets that the DAGs of the agoranomos bundle share.

A DAG file takes an asset from here and never from another DAG file, which would define the DAGs of
that file twice.
"""

from airflow.sdk import Asset, AssetAll

from agoranomos_pipelines.demo.platforms import PLATFORMS, asset_uri

# The assets of the main tables of the five fictional platforms of the demo, in the form that
# Airflow 3 requires for PostgreSQL: postgres://<host>:<port>/<database>/<schema>/<table>. Each
# load of a platform updates its own. The dbt profile uses the same host, port and database, so the
# assets that Cosmos emits for the models have the same form.
RAW_DEMO_PLATFORMS = {code: Asset(asset_uri(platform)) for code, platform in PLATFORMS.items()}

# The condition on which the transform runs: each platform of the demo has loaded since its last
# run. A schedule that is a list means the same to Airflow; as one condition, it can be combined
# with another asset by | or &.
DEMO_LOADED = AssetAll(*RAW_DEMO_PLATFORMS.values())

# The asset of the rates of the ECB (ADR-0032), in the same form, which each of its loads updates.
# The transform does not wait on it: it takes the rates at its next run.
RAW_ECB = Asset("postgres://warehouse-db:5432/warehouse/raw_ecb/rates_daily")

# The asset of the statistics of Google Ads (ADR-0032), in the same form, which each of its loads
# updates. The transform runs when the demo has loaded or when Google Ads has.
RAW_GOOGLE_ADS = Asset("postgres://warehouse-db:5432/warehouse/raw_google_ads/campaign_stats")
LOADED = DEMO_LOADED | RAW_GOOGLE_ADS
