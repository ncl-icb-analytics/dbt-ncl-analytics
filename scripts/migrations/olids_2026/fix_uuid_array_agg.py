"""Wrap UUID column refs in ARRAY_AGG(DISTINCT ...) with ::VARCHAR.

Snowflake 2026 hits internal error 300010 when running
ARRAY_AGG(DISTINCT <uuid_column>) — surfaced by the 2026 OLIDS schema
realignment which retyped every ID column TEXT -> UUID. Casting to
VARCHAR is the documented workaround.

Heuristic: match ARRAY_AGG(DISTINCT <ident>) where <ident> ends in
`_id` or `_ID` and is a bare column reference (no existing cast,
no function call). Conservative — only touches the unambiguous case.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parents[3]

# ARRAY_AGG ( DISTINCT [alias.]colname )
# Captures: whole match, optional alias prefix, column name
# Matches column names ending in `_id` OR the bare names `id` / `ID`
# (both spellings appear in the codebase). Conservative — only touches
# bare identifiers, no function calls or existing casts.
PATTERN = re.compile(
    r"""
    (ARRAY_AGG\s*\(\s*DISTINCT\s+)
    ((?:[a-z_][a-z0-9_]*\.)?      # optional alias.
     (?:[a-z_][a-z0-9_]*_id       # ...colname ending in _id
        |id))                     # OR bare 'id'
    (\s*\))                       # close paren
    """,
    re.IGNORECASE | re.VERBOSE,
)


def rewrite(text: str) -> tuple[str, int]:
    n = 0
    def _sub(m):
        nonlocal n
        n += 1
        return f"{m.group(1)}{m.group(2)}::VARCHAR{m.group(3)}"
    return PATTERN.sub(_sub, text), n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--paths", nargs="*", default=["models"])
    args = ap.parse_args()

    total_files = 0
    total_subs = 0
    for root in args.paths:
        for sql_path in (REPO / root).rglob("*.sql"):
            text = sql_path.read_text(encoding="utf-8", errors="replace")
            new_text, n = rewrite(text)
            if n:
                rel = sql_path.relative_to(REPO)
                print(f"  {rel}: {n} substitution(s)")
                total_files += 1
                total_subs += n
                if not args.dry_run:
                    sql_path.write_text(new_text, encoding="utf-8", newline="\n")

    print(f"\nFiles touched: {total_files}  | Total substitutions: {total_subs}")
    if args.dry_run:
        print("[DRY RUN] No files were written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
