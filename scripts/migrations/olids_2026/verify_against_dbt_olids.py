"""Diff staging-model SELECT lists against the dbt-olids stable layer.

For every `stg_olids_<table>` in this repo, find the matching
`stable_<table>` in `C:/Projects/dbt-olids/models/olids/stable/`, parse both
SELECT column lists, and report:

  - columns in staging that don't exist in stable (would fail at runtime)
  - columns in stable that we don't expose (potential additions)

Naive SQL parser — assumes each SELECT column lives on its own line and the
column body is either a bare identifier, an `expr AS alias` form, or starts
with `--` (skipped). Sufficient for the regular shape both projects use.
"""

from __future__ import annotations

import pathlib
import re
import sys

ANALYTICS_DIR = pathlib.Path(__file__).resolve().parents[3] / "models" / "staging" / "olids"
OLIDS_STABLE_DIR = pathlib.Path("C:/Projects/dbt-olids/models/olids/stable")

# Map staging filename stem -> stable filename stem
def stable_for(stg_stem: str) -> str | None:
    """`stg_olids_appointment` -> `stable_appointment`."""
    if not stg_stem.startswith("stg_olids_"):
        return None
    return "stable_" + stg_stem[len("stg_olids_"):]


COL_LINE = re.compile(r"^\s*([a-z_][a-z0-9_]*)\s*(,?)\s*(--.*)?$", re.IGNORECASE)
AS_LINE = re.compile(r"^\s*(.+?\s+as\s+)([a-z_][a-z0-9_]*)\s*(,?)\s*(--.*)?$", re.IGNORECASE)
SQL_KEYWORDS = {
    "select", "from", "where", "qualify", "and", "or", "distinct",
    "left", "right", "inner", "outer", "join", "on", "group", "by",
    "having", "order", "limit", "with",
}


SOURCE_COL_IN_AS_LINE = re.compile(r"^\s*([a-z_][a-z0-9_]*)\s+as\s+", re.IGNORECASE)


def extract_select_cols(text: str) -> tuple[list[str], list[str]]:
    """Return (output_names, source_names) for the topmost SELECT ... FROM.

    output_names = the alias (or bare identifier) — what downstream consumers see.
    source_names = the column read from upstream — what must exist in `from` table.
    For a bare `foo` line both are the same. For `foo as bar`, source=foo, output=bar.
    """
    outputs: list[str] = []
    sources: list[str] = []
    in_select = False
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip().lower()
        if not in_select and stripped.startswith("select"):
            in_select = True
            continue
        if in_select and re.match(r"\s*from\b", line, re.IGNORECASE):
            break
        if not in_select:
            continue
        if not stripped or stripped.startswith("--"):
            continue
        # Try `... AS alias` first
        m = AS_LINE.match(line)
        if m:
            output = m.group(2).lower()
            src_m = SOURCE_COL_IN_AS_LINE.match(line)
            source = src_m.group(1).lower() if src_m else output
        else:
            m = COL_LINE.match(line)
            if not m:
                continue
            output = source = m.group(1).lower()
        if output in SQL_KEYWORDS:
            continue
        outputs.append(output)
        sources.append(source)
    return outputs, sources


def main() -> int:
    if not OLIDS_STABLE_DIR.exists():
        print(f"ERROR: dbt-olids stable dir not found: {OLIDS_STABLE_DIR}", file=sys.stderr)
        return 1

    total_unknown = 0
    total_unexposed = 0
    for stg_sql in sorted(ANALYTICS_DIR.glob("stg_olids_*.sql")):
        stable_stem = stable_for(stg_sql.stem)
        if stable_stem is None:
            continue
        stable_sql = OLIDS_STABLE_DIR / f"{stable_stem}.sql"
        if not stable_sql.exists():
            print(f"\n=== {stg_sql.stem} ===")
            print(f"  NO MATCHING STABLE MODEL ({stable_stem}.sql not found)")
            continue

        _, stg_sources = extract_select_cols(stg_sql.read_text(encoding="utf-8"))
        stable_outputs, _ = extract_select_cols(stable_sql.read_text(encoding="utf-8"))
        stg_source_set = set(stg_sources)
        stable_set = set(stable_outputs)

        # A staging column is "unknown" only if the SOURCE column doesn't exist
        # in stable. The alias side is the downstream contract, so we don't care
        # what staging chose to call it.
        unknown = sorted(stg_source_set - stable_set)
        unexposed = sorted(stable_set - stg_source_set)

        if not (unknown or unexposed):
            continue

        print(f"\n=== {stg_sql.stem} vs {stable_stem} ===")
        if unknown:
            print(f"  UNKNOWN in upstream stable (staging refs columns that don't exist):")
            for c in unknown:
                print(f"    - {c}")
            total_unknown += len(unknown)
        if unexposed:
            print(f"  NOT EXPOSED (in stable but not selected by staging):")
            for c in unexposed:
                print(f"    + {c}")
            total_unexposed += len(unexposed)

    print(f"\n=== Summary ===")
    print(f"  Total unknown-upstream columns (will FAIL at runtime): {total_unknown}")
    print(f"  Total unexposed-upstream columns (potential additions): {total_unexposed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
