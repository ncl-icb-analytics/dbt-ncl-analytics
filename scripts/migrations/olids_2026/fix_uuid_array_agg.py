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

# Path fragments — only files under these paths get the UUID cast. The non-OLIDS
# `_id` columns (waiting lists, PDS, SUS etc.) are NUMBER-typed and don't need
# (and shouldn't get) the cast.
OLIDS_PATH_FRAGMENTS = [
    "models/staging/olids/",
    "models/raw/olids/",
    "models/modelling/olids/",
    "models/reporting/olids/",
    "models/published/direct_care/C_LTCS/",  # consumes OLIDS
    "models/semantic/",  # semantic views over OLIDS
    "macros/clinical/",  # clinical helpers wrap OLIDS columns
]

# UUID columns can't be used as-is in ARRAY_AGG, MAX/MIN, or ORDER BY in
# Snowflake 2026. Each pattern below wraps the column reference in ::VARCHAR.
# Conservative — only touches bare identifiers ending in `_id` or `id` (the
# UUID-typed columns from the 2026 OLIDS retyping), and only when not
# already cast (the `(?!::)` lookahead).

_COL = r"(?:[a-z_][a-z0-9_]*\.)?(?:[a-z_][a-z0-9_]*_id|id)"

PATTERNS = [
    # ARRAY_AGG(DISTINCT <col>) — preserved trailing ) check needed
    (re.compile(
        rf"(ARRAY_AGG\s*\(\s*DISTINCT\s+)({_COL})(?!::)(\s*\))",
        re.IGNORECASE,
    ), r"\1\2::VARCHAR\3"),

    # WITHIN GROUP (ORDER BY <col>) — keep ORDER BY direction etc. simple
    # by anchoring on `ORDER BY <col>` followed by optional whitespace + `)`
    (re.compile(
        rf"(WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+)({_COL})(?!::)(\s*(?:ASC|DESC)?\s*\))",
        re.IGNORECASE,
    ), r"\1\2::VARCHAR\3"),

    # MAX/MIN(<col>) — direct aggregate on a UUID column
    (re.compile(
        rf"(\b(?:MAX|MIN)\s*\(\s*)({_COL})(?!::)(\s*\))",
        re.IGNORECASE,
    ), r"\1\2::VARCHAR\3"),

    # MAX/MIN(CASE WHEN ... THEN <col> ELSE NULL END) — wrap the column
    # reference inside the THEN branch. The CASE body is typically short and
    # doesn't contain nested CASE, so a non-greedy `.+?` is safe enough here.
    (re.compile(
        rf"(\b(?:MAX|MIN)\s*\(\s*CASE\s+WHEN\s+.+?\s+THEN\s+)({_COL})(?!::)(\s+(?:ELSE\s+NULL\s+)?END\s*\))",
        re.IGNORECASE | re.DOTALL,
    ), r"\1\2::VARCHAR\3"),
]


def rewrite(text: str) -> tuple[str, int]:
    total = 0
    for pat, repl in PATTERNS:
        text, n = pat.subn(repl, text)
        total += n
    return text, total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--paths", nargs="*", default=["models"])
    args = ap.parse_args()

    total_files = 0
    total_subs = 0
    for root in args.paths:
        for sql_path in (REPO / root).rglob("*.sql"):
            rel = str(sql_path.relative_to(REPO)).replace("\\", "/")
            if not any(rel.startswith(frag) for frag in OLIDS_PATH_FRAGMENTS):
                continue
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
