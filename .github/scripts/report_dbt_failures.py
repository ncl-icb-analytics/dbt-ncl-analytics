"""Create, update, or resolve dbt failure issues from run_results.json.

Called after every dbt build (deploy and scheduled workflows) with:
  DBT_CONTEXT   "deploy" or a scheduled run type (daily, weekly, ...)
  BUILD_OUTCOME "success" or "failure" (the dbt step's outcome)
  RUN_URL       link to the workflow run
  GH_TOKEN      token with issues:write

On failure: opens (or comments on) an issue listing the failed nodes with
their error messages, plus a machine-readable failed-nodes block.

On success:
  - scheduled contexts: close the run type's issue (same selection retried
    each run, so a green run means genuine recovery)
  - deploy: close an issue only when every node it names has since built
    green or left the project - unrelated green deploys skip broken nodes
    (publish-always state baseline), so "next green run" proves nothing
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

CONTEXT = os.environ["DBT_CONTEXT"]
OUTCOME = os.environ["BUILD_OUTCOME"]
RUN_URL = os.environ["RUN_URL"]
REPO = os.environ["GITHUB_REPOSITORY"]
LABEL = os.environ.get("ISSUE_LABEL", "dbt-run-failure")

IS_DEPLOY = CONTEXT == "deploy"
TITLE = "dbt deploy failing" if IS_DEPLOY else f"Scheduled dbt run failing: {CONTEXT}"
SUCCESS_STATUSES = {"success", "pass"}
MAX_LISTED = 20


def gh(*args: str) -> str:
    r = subprocess.run(["gh", *args], check=True, text=True, capture_output=True, encoding="utf-8")
    return r.stdout


def load_results() -> list[dict]:
    try:
        with open("target/run_results.json", encoding="utf-8") as f:
            return json.load(f).get("results", [])
    except OSError:
        return []


def manifest_node_ids() -> set[str]:
    try:
        with open("target/manifest.json", encoding="utf-8") as f:
            m = json.load(f)
        return set(m.get("nodes", {})) | set(m.get("sources", {}))
    except OSError:
        return set()


def find_open_issues() -> list[dict]:
    out = gh("issue", "list", "--repo", REPO, "--label", LABEL, "--state", "open",
             "--search", f'in:title "{TITLE}"', "--json", "number,title")
    return [i for i in json.loads(out) if i["title"] == TITLE]


def failed_node_blocks(issue_number: int) -> set[str]:
    """Union of unique_ids from every ```failed-nodes block in body + comments."""
    out = gh("issue", "view", str(issue_number), "--repo", REPO, "--json", "body,comments")
    data = json.loads(out)
    texts = [data.get("body") or ""] + [c.get("body") or "" for c in data.get("comments", [])]
    ids: set[str] = set()
    for t in texts:
        for block in re.findall(r"```failed-nodes\n(.*?)```", t, flags=re.DOTALL):
            ids.update(line.strip() for line in block.splitlines() if line.strip())
    return ids


def short(uid: str) -> str:
    return uid.split(".")[-1]


def report_failure() -> None:
    results = load_results()
    failed = [r for r in results if r.get("status") in ("error", "fail")]
    skipped = [r for r in results if r.get("status") == "skipped"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"[{CONTEXT} run failed]({RUN_URL}) at {now}.", ""]
    if failed:
        lines.append("Failed nodes:")
        for r in failed[:MAX_LISTED]:
            msg = (r.get("message") or "").replace("\n", " ").strip()
            lines.append(f"- `{short(r['unique_id'])}` ({r['status']}): {msg[:250]}")
        if len(failed) > MAX_LISTED:
            lines.append(f"- ...and {len(failed) - MAX_LISTED} more")
        if skipped:
            lines.append(f"- plus {len(skipped)} skipped downstream nodes")
        lines += ["", "```failed-nodes"]
        lines += sorted(r["unique_id"] for r in failed)
        lines.append("```")
    else:
        lines.append("No run_results available - the build died before or during parse; see the run log.")

    body = "\n".join(lines)
    gh("label", "create", LABEL, "--repo", REPO, "--color", "B60205",
       "--description", "dbt prod run failure", "--force")
    issues = find_open_issues()
    if issues:
        gh("issue", "comment", str(issues[0]["number"]), "--repo", REPO, "--body", body)
        print(f"commented on issue #{issues[0]['number']}")
    else:
        if IS_DEPLOY:
            body += ("\n\nFailed nodes are not retried by unrelated deploys; this issue closes "
                     "automatically once every node listed here builds green (its fix merged) "
                     "or is removed from the project.")
        else:
            body += f"\n\nAuto-closes on the next successful {CONTEXT} run."
        gh("issue", "create", "--repo", REPO, "--title", TITLE, "--label", LABEL, "--body", body)
        print("created issue")


def resolve_success() -> None:
    issues = find_open_issues()
    if not issues:
        print("no open failure issue")
        return

    if not IS_DEPLOY:
        for i in issues:
            gh("issue", "close", str(i["number"]), "--repo", REPO,
               "--comment", f"Recovered: [successful {CONTEXT} run]({RUN_URL}).")
            print(f"closed issue #{i['number']}")
        return

    built_green = {r["unique_id"] for r in load_results() if r.get("status") in SUCCESS_STATUSES}
    current_nodes = manifest_node_ids()
    for i in issues:
        tracked = failed_node_blocks(i["number"])
        unresolved = {uid for uid in tracked if uid in current_nodes and uid not in built_green}
        if tracked and not unresolved:
            gh("issue", "close", str(i["number"]), "--repo", REPO,
               "--comment", f"All tracked nodes built green or were removed - [run]({RUN_URL}).")
            print(f"closed issue #{i['number']}")
        else:
            print(f"issue #{i['number']} stays open ({len(unresolved)} nodes still broken)")


if OUTCOME == "failure":
    report_failure()
elif OUTCOME == "success":
    resolve_success()
else:
    print(f"outcome {OUTCOME} - nothing to do")
sys.exit(0)
