# The image of the container ai (ADR-0030): the MCP server of the warehouse, MCP Toolbox for
# Databases, and that of Airflow, astro-airflow-mcp. ./agoranomos up builds it, from the cache of
# Docker after the first time, like the Airflow image; it is never pulled from or pushed to a
# registry. The tools of Toolbox and the script that starts a server come from the checkout,
# read-only (compose.yaml).

# Toolbox publishes no binary for Linux on arm64, but its official image has one for amd64 and arm64
# (as of version 1.15.0, October 2026): the binary is taken from there, for the architecture of the
# build.
FROM us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:1.15.0@sha256:7e4f7bfd93ccec33c066563a4ca700572e4ce95a8d45ba18577df8ee50b9481a AS toolbox

FROM python:3.13.16-slim-trixie@sha256:3dd7cc108ec1493442514f5c2a871af6af0ec31d768ff6e378a93340c3b3db5f

# astro-airflow-mcp and its dependencies, from the lock file with hashes, with no other package.
COPY requirements.txt /tmp/agoranomos/requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r /tmp/agoranomos/requirements.txt \
    && pip check \
    && rm -rf /tmp/agoranomos

COPY --from=toolbox /toolbox /usr/local/bin/toolbox
