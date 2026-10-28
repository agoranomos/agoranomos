#!/bin/sh
# Starts one MCP server of the container ai on stdio (ADR-0030): ./agoranomos mcp <server> runs it
# with docker compose exec, so that its standard input and output are those of the AI client. A
# server whose switch in .env is off does not start: the switch is one limit, and the account behind
# the server, ai_reader in the warehouse or the user ai in Airflow, is the other. Everything but the
# protocol goes to the standard error.
set -eu

case "${1:-}" in
  warehouse)
    if [ "${WAREHOUSE_MCP:-false}" != true ]; then
      echo "The MCP server of the warehouse is off: ./agoranomos ai warehouse on turns it on" >&2
      exit 1
    fi
    # The network of the container has no route to the internet, so the check for a newer version
    # would only wait. At the level warn, Toolbox 1.15.0 warns on every start about the origins and
    # the hosts that its HTTP server allows, which stdio does not use.
    exec toolbox --stdio --config /opt/agoranomos/ai/tools.yaml --disable-reload \
      --disable-version-check --log-level error
    ;;
  airflow)
    if [ "${AIRFLOW_MCP:-false}" != true ]; then
      echo "The MCP server of Airflow is off: ./agoranomos ai airflow on turns it on" >&2
      exit 1
    fi
    exec astro-airflow-mcp --transport stdio
    ;;
  *)
    echo "usage: mcp.sh warehouse|airflow" >&2
    exit 2
    ;;
esac
