# The Airflow image of Agoranomos: the official image, with dlt and Cosmos in Airflow's own environment,
# dbt in a virtual environment of its own (ADR-0002, ADR-0005), and the plugins of airflow/plugins.
# ./agoranomos up builds it from this folder, and it is never pushed to a registry.
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

# The two caches of Cosmos stay out of Airflow Variables, which the Op role may edit. A DAG of Cosmos
# that reads its project with dbt ls would keep the output there and render the tasks of dbt and
# their arguments from it, so such a Variable could change what a dbt task runs. The transform DAG
# needs neither: Cosmos renders it from the manifest of the dbt project, which metadata-export
# writes into a volume that the Airflow services mount read-only (ADR-0027). compose.yaml sets the
# same.
ENV AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False \
    AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS=False

# dlt and Cosmos go into Airflow's environment, under the constraints that the base image was built
# with, and with apache-airflow in the same command, so that pip fails instead of moving Airflow.
# requirements.txt also pins the dependencies of dlt that the constraints leave open.
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

# The folders that the metadata-export service writes the export and the manifest into, as nobody
# (65534): a new named volume takes the owner of the folder that it is first mounted on, and without
# these folders Docker would give the volumes to root, and the job could not write them
# (compose.yaml). The folder of the plugins is made here too, as root, for the COPY below. These
# come last, so that the layers above stay in the cache of Docker. The image then runs as the user
# of the base.
USER root
RUN install -d -o 65534 -g 65534 -m 0755 /opt/agoranomos/metadata /opt/agoranomos/manifest
RUN install -d -m 0755 /opt/agoranomos/plugins
USER ${AIRFLOW_UID}

# The plugins of Agoranomos: agoranomos_oom.py makes each task process the first that the kernel
# stops when its container reaches its memory limit, before the scheduler or the worker, and
# serve-logs and the health check of the scheduler the next, on a host whose kernel restricts
# ptrace with Yama (the docstring of the plugin says why). Airflow
# imports every file of its plugins folder in each of its processes, so the folder and the plugins
# belong to root, and only root may write them: the Airflow services run as 50000 in the group root,
# COPY would keep a mode of the checkout that the group may write, and the default folder,
# /opt/airflow/plugins, would be in a folder that the group may write. The folder is made above and
# not by COPY: with --chmod, COPY gives a folder that it creates the mode of the file, 0644, and the
# Airflow services could not enter /opt/agoranomos, where the DAGs and the dbt project are mounted.
COPY --chmod=0644 plugins/agoranomos_oom.py /opt/agoranomos/plugins/agoranomos_oom.py
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/agoranomos/plugins
