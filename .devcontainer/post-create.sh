#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks

# Install the dbt Fusion engine (dbt runs on Fusion, not a Python package).
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
export PATH="$HOME/.local/bin:$PATH"

# Python tooling for the helper scripts in scripts/ (not needed for dbt itself).
if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi
uv sync

# dbt packages
dbt deps || true

if [ -n "${SNOWFLAKE_PAT:-}" ]; then
    echo "Codespaces bootstrap complete. Snowflake PAT detected."
else
    echo "Codespaces bootstrap complete. Add Snowflake secrets before running dbt debug."
fi
