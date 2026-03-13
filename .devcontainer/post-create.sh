#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks

if command -v uv >/dev/null 2>&1; then
    uv sync
    uv run dbt deps
else
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    dbt deps
fi

if [ -n "${SNOWFLAKE_PAT:-}" ]; then
    echo "Codespaces bootstrap complete. Snowflake PAT detected."
else
    echo "Codespaces bootstrap complete. Add Snowflake secrets before running dbt debug."
fi
