# The Airflow image of Agoranomos: the official image, with dlt and Cosmos in Airflow's own environment
# and dbt in a virtual environment of its own (ADR-0002, ADR-0005). ./agoranomos up builds it from this
# folder, and it is never pushed to a registry.
FROM apache/airflow:3.3.2-python3.13@sha256:e9982ad3f49a60418622e1baf80c34b0daf3fa5091a336cb5ee61f0cdd189371

# No usage telemetry from the tools that this image adds; compose.yaml sets the same variables.
ENV RUNTIME__DLTHUB_TELEMETRY=false \
    DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS=False \
    AIRFLOW__COSMOS__ENABLE_TELEMETRY=False \
    DO_NOT_TRACK=1

# Inside an Airflow task, dlt also takes its settings from Airflow Variables, which the Op role may
# create: such a Variable could send the connection of a load, with its password, to another host.
# The loads hand dlt their connection themselves and need no Variable, so dlt reads none;
# compose.yaml sets the same.
ENV PROVIDERS__ENABLE_AIRFLOW_SECRETS=false

# The two caches from which Cosmos renders the tasks of dbt stay out of Airflow Variables, which the
# Op role may edit. Cosmos would keep the output of dbt ls there and render the tasks of dbt and
# their arguments from it, so such a Variable could change what a dbt task runs. Instead, dbt ls
# runs each time that the DAG processor parses a DAG of Cosmos, and again when each task of that
# DAG starts, since a task parses the file of its DAG first; compose.yaml sets the same.
ENV AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False \
    AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS=False

# dlt and Cosmos go into Airflow's environment, under the constraints that the base image was built
# with, and with apache-airflow in the same command, so that pip fails instead of moving Airflow.
COPY requirements.txt /tmp/agoranomos/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/agoranomos/requirements.txt --constraint "${HOME}/constraints.txt" \
    && pip check

# dbt gets a virtual environment of its own: two pins of Airflow's constraints, pathspec and
# more-itertools, conflict with dbt. The lock file pins every package with its hashes. dbt-core 1.12
# depends on dbt-core-experimental-parser, whose build downloads a wheel from GitHub Releases and
# checks its SHA-256, so the build needs GitHub as well as PyPI.
COPY dbt-requirements.txt /tmp/agoranomos/dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --require-hashes \
        -r /tmp/agoranomos/dbt-requirements.txt \
    && /opt/airflow/dbt_venv/bin/pip check
