"""The assets that the DAGs of the agoranomos bundle share.

A DAG file takes an asset from here and never from another DAG file, which would define the DAGs of
that file twice.
"""

from airflow.sdk import Asset

# The asset of the raw table of the demo, in the form that Airflow 3 requires for PostgreSQL:
# postgres://<host>:<port>/<database>/<schema>/<table>. agoranomos_demo_load updates it, and
# agoranomos_demo_transform is scheduled on it. The dbt profile uses the same host, port and
# database, so the assets that Cosmos emits for the models have the same form.
RAW_DEMO = Asset("postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily")
