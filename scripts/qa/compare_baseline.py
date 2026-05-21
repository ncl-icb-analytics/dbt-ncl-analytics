"""Compare two row-count baselines and report drift.

Sources can be the Snowflake baseline table (default) or two JSON snapshots.
Output is a human-readable diff sorted by absolute drift, plus an optional
CSV/JSON dump.
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
from dataclasses import dataclass
from typing import Optional

import snowflake.connector

BASELINE_TABLE = "DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_ROW_COUNT_BASELINE"


@dataclass
class Row:
    database_name: str
    schema_name: str
    model_name: str
    table_type: str
    row_count: Optional[int]
    source: str
    error: Optional[str]

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.database_name, self.schema_name, self.model_name)


def load_from_snowflake(connection: str, label: str) -> dict[tuple[str, str, str], Row]:
    conn = snowflake.connector.connect(connection_name=connection, authenticator="externalbrowser")
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT database_name, schema_name, model_name, table_type, "
                f"row_count, source, error FROM {BASELINE_TABLE} WHERE label = %s",
                (label,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()
    out: dict[tuple[str, str, str], Row] = {}
    for db, sch, name, ttype, cnt, src, err in rows:
        r = Row(db, sch, name, ttype, int(cnt) if cnt is not None else None, src, err)
        out[r.key] = r
    return out


def load_from_json(path: pathlib.Path) -> dict[tuple[str, str, str], Row]:
    payload = json.loads(path.read_text())
    out: dict[tuple[str, str, str], Row] = {}
    for m in payload["models"]:
        r = Row(
            database_name=m["database_name"],
            schema_name=m["schema_name"],
            model_name=m["model_name"],
            table_type=m["table_type"],
            row_count=m.get("row_count"),
            source=m.get("source", ""),
            error=m.get("error"),
        )
        out[r.key] = r
    return out


def fmt_pct(before: Optional[int], after: Optional[int]) -> str:
    if before is None or after is None or before == 0:
        return "n/a"
    return f"{(after - before) / before * 100:+.2f}%"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--before", required=True, help="Label or JSON path of pre-change baseline")
    ap.add_argument("--after", required=True, help="Label or JSON path of post-change baseline")
    ap.add_argument(
        "--connection",
        default="data-platform-manager",
        help="Snowflake connection_name (used when --before/--after are labels)",
    )
    ap.add_argument(
        "--threshold-pct",
        type=float,
        default=0.0,
        help="Hide unchanged or sub-threshold drift (default: show everything that changed)",
    )
    ap.add_argument(
        "--ignore",
        nargs="*",
        default=["raw_wl_wl_clockstops_data", "int_myria_conditions"],
        help="Model names to exclude (default: known fails)",
    )
    ap.add_argument("--csv-out", type=pathlib.Path, help="Optional CSV dump of full diff")
    ap.add_argument("--json-out", type=pathlib.Path, help="Optional JSON dump of full diff")
    ap.add_argument("--top", type=int, default=50, help="Top N rows to print to console")
    args = ap.parse_args()

    def _load(ref: str) -> dict[tuple[str, str, str], Row]:
        p = pathlib.Path(ref)
        if p.exists() and p.suffix == ".json":
            return load_from_json(p)
        return load_from_snowflake(args.connection, ref)

    before = _load(args.before)
    after = _load(args.after)

    print(f"Before: {len(before):,} models  |  After: {len(after):,} models")

    keys = sorted(set(before) | set(after))
    diffs = []
    for k in keys:
        b = before.get(k)
        a = after.get(k)
        if b and a and b.model_name in args.ignore:
            continue
        b_count = b.row_count if b else None
        a_count = a.row_count if a else None
        delta = None
        if b_count is not None and a_count is not None:
            delta = a_count - b_count
        status = (
            "MISSING_IN_BEFORE" if b is None
            else "MISSING_IN_AFTER" if a is None
            else "ERROR_IN_BEFORE" if b.error
            else "ERROR_IN_AFTER" if a.error
            else "OK"
        )
        diffs.append({
            "database_name": k[0],
            "schema_name": k[1],
            "model_name": k[2],
            "status": status,
            "before": b_count,
            "after": a_count,
            "delta": delta,
            "pct": fmt_pct(b_count, a_count),
            "before_error": b.error if b else None,
            "after_error": a.error if a else None,
        })

    changed = [d for d in diffs if d["status"] != "OK" or (d["delta"] is not None and d["delta"] != 0)]

    if args.threshold_pct > 0:
        def _abs_pct(d):
            if d["before"] in (None, 0) or d["after"] is None:
                return float("inf")  # always show structural issues
            return abs((d["after"] - d["before"]) / d["before"] * 100)
        changed = [d for d in changed if _abs_pct(d) >= args.threshold_pct]

    changed.sort(
        key=lambda d: (
            0 if d["status"] == "OK" else 1,
            -abs(d["delta"]) if d["delta"] is not None else float("inf"),
        ),
        reverse=True,
    )
    # Surface structural issues first, then largest absolute drift
    changed.sort(key=lambda d: (
        0 if d["status"] != "OK" else 1,
        -abs(d["delta"]) if d["delta"] is not None else 0,
    ))

    print(f"\n{len(changed):,} models with drift or status changes")
    print(f"{'STATUS':<20}{'DB':<35}{'SCHEMA':<32}{'MODEL':<50}{'BEFORE':>14}{'AFTER':>14}{'DELTA':>14}  PCT")
    for d in changed[: args.top]:
        before_s = f"{d['before']:,}" if d["before"] is not None else "-"
        after_s = f"{d['after']:,}" if d["after"] is not None else "-"
        delta_s = f"{d['delta']:+,}" if d["delta"] is not None else "-"
        print(
            f"{d['status']:<20}{d['database_name'][:34]:<35}{d['schema_name'][:31]:<32}"
            f"{d['model_name'][:49]:<50}{before_s:>14}{after_s:>14}{delta_s:>14}  {d['pct']}"
        )

    if args.csv_out:
        with args.csv_out.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(diffs[0].keys()))
            w.writeheader()
            w.writerows(diffs)
        print(f"\nCSV: {args.csv_out}")

    if args.json_out:
        args.json_out.write_text(json.dumps(diffs, indent=2, default=str))
        print(f"JSON: {args.json_out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
