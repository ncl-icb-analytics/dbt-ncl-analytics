"""Upload dbt artifacts to the DBT_ARTIFACTS stage.

Puts target/manifest.json + target/run_results.json (auto-compressed to .gz)
and a meta.json sidecar to @<stage>/<target>/latest/, overwriting the previous
set. Auth: key-pair via SNOWFLAKE_PRIVATE_KEY_PATH (same env as the workflows).
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

import snowflake.connector

STAGE = os.environ["ARTIFACT_STAGE"]
TARGET = os.environ["DBT_TARGET"]

if not re.match(r"^[A-Za-z0-9_.]+$", STAGE) or not re.match(r"^[A-Za-z0-9_-]+$", TARGET):
    sys.exit(f"Invalid stage/target: {STAGE} / {TARGET}")

manifest = "target/manifest.json"
run_results = "target/run_results.json"
if not os.path.exists(manifest):
    sys.exit(f"{manifest} not found - nothing to publish")

fusion_version = None
if os.path.exists(".fusion-version"):
    with open(".fusion-version") as f:
        fusion_version = f.read().strip()

meta = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "commit_sha": os.environ.get("GITHUB_SHA"),
    "ref": os.environ.get("GITHUB_REF_NAME"),
    "run_id": os.environ.get("GITHUB_RUN_ID"),
    "run_url": "{server}/{repo}/actions/runs/{run}".format(
        server=os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
        repo=os.environ.get("GITHUB_REPOSITORY", ""),
        run=os.environ.get("GITHUB_RUN_ID", ""),
    ),
    "workflow": os.environ.get("GITHUB_WORKFLOW"),
    "dbt_command": os.environ.get("DBT_COMMAND"),
    "target": TARGET,
    "fusion_version": fusion_version,
}
with open("target/meta.json", "w") as f:
    json.dump(meta, f, indent=2)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    private_key_file_pwd=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None,
    role="DBT_ADMIN",
    warehouse="WH_NCL_ENGINEERING_XS",
)
try:
    cur = conn.cursor()
    dest = f"@{STAGE}/{TARGET}/latest/"
    uploads = [(manifest, True), ("target/meta.json", False)]
    if os.path.exists(run_results):
        uploads.insert(1, (run_results, True))
    else:
        print(f"{run_results} not found - skipping")
    for path, compress in uploads:
        path = os.path.abspath(path).replace("\\", "/")
        cur.execute(
            f"PUT 'file://{path}' '{dest}' "
            f"AUTO_COMPRESS={'TRUE' if compress else 'FALSE'} OVERWRITE=TRUE"
        )
        print(f"Uploaded {path} -> {dest} ({cur.fetchall()[0][6]})")
finally:
    conn.close()
