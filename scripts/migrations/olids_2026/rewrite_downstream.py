"""Rewrite downstream references to OLIDS columns renamed in column_renames.yml.

For each .sql/.yml file under models/{modelling,reporting,published} and macros/:
  1. Find which `ref('stg_olids_*')` (or `raw_olids_*`) it depends on.
  2. Union the rename maps for those tables (plus cross-cutting defaults).
  3. If a column name maps to the SAME new name across all in-scope tables,
     do a whole-word substitution.
  4. If a column name maps to DIFFERENT new names depending on which staging
     model it came from, flag the file for manual review and do not rewrite
     that column.

Idempotent. Prints a report at the end with:
  - files touched (with rename counts)
  - files flagged for manual review (with the ambiguous columns)
  - removed columns that still appear (manual fixes required)
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field

import yaml

REPO = pathlib.Path(__file__).resolve().parents[3]
RENAMES_FILE = pathlib.Path(__file__).with_name("column_renames.yml")
SCAN_DIRS = [
    REPO / "models" / "modelling",
    REPO / "models" / "reporting",
    REPO / "models" / "published",
    REPO / "models" / "semantic",
    REPO / "macros",
]

REF_PATTERN = re.compile(
    r"""\{\{\s*ref\(\s*['"](stg_olids_\w+|raw_olids_\w+)['"]\s*\)\s*\}\}""",
    re.IGNORECASE,
)
# Hardcoded references such as DBT_STAGING.STG_OLIDS_* / DBT_RAW.RAW_OLIDS_*
HARDCODED_PATTERN = re.compile(
    r"""\b(?:DBT_STAGING|DBT_RAW)\.(STG_OLIDS_\w+|RAW_OLIDS_\w+)\b""",
    re.IGNORECASE,
)
# `ref('stg_olids_X')` followed by an optional `as` and an alias identifier.
# Captures (table, alias).
REF_ALIAS_PATTERN = re.compile(
    r"""\{\{\s*ref\(\s*['"](stg_olids_\w+|raw_olids_\w+)['"]\s*\)\s*\}\}\s+(?:as\s+)?([a-z_]\w*)""",
    re.IGNORECASE,
)
# Same idea for hardcoded `DBT_STAGING.STG_OLIDS_X alias` / `DBT_RAW.RAW_OLIDS_X alias`.
HARDCODED_ALIAS_PATTERN = re.compile(
    r"""\b(?:DBT_STAGING|DBT_RAW)\.(STG_OLIDS_\w+|RAW_OLIDS_\w+)\s+(?:as\s+)?([a-z_]\w*)""",
    re.IGNORECASE,
)


@dataclass
class RenameContext:
    # column -> set of new names found across in-scope tables (for ambiguity detection)
    rename_targets: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Per-table renames (old -> new) for alias-aware qualified rewrites
    per_table_renames: dict[str, dict[str, str]] = field(default_factory=dict)
    removed_cols: set[str] = field(default_factory=set)
    in_scope_tables: set[str] = field(default_factory=set)
    # The cross-cutting (defaults) rename keys — safe to rewrite unqualified
    cross_cutting_keys: set[str] = field(default_factory=set)

    def add_table(self, name: str, renames: dict[str, str], removes: list[str]) -> None:
        self.in_scope_tables.add(name)
        self.per_table_renames[name] = renames
        for old, new in renames.items():
            if old == new:
                continue
            self.rename_targets[old].add(new)
        for col in removes:
            self.removed_cols.add(col)


def load_rules():
    data = yaml.safe_load(RENAMES_FILE.read_text())
    defaults_renames = data.get("defaults", {}).get("renames", {}) or {}
    defaults_removes = data.get("defaults", {}).get("removes", []) or []
    table_rules = {}
    for name, cfg in (data.get("tables") or {}).items():
        cfg = cfg or {}
        renames = dict(defaults_renames)
        for k, v in (cfg.get("overrides") or {}).items():
            if k == v:
                renames.pop(k, None)
            else:
                renames[k] = v
        for k, v in (cfg.get("renames") or {}).items():
            renames[k] = v
        removes = list(defaults_removes) + [r for r in (cfg.get("removes") or []) if r]
        table_rules[name] = {
            "renames": renames,
            "removes": removes,
            "delete": bool(cfg.get("delete", False)),
        }
        # Also accept raw_olids_X aliases so hardcoded raw refs get covered
        raw = cfg.get("raw")
        if raw:
            table_rules[raw] = table_rules[name]
    return table_rules, set(defaults_renames.keys())


def collect_refs(text: str) -> set[str]:
    out: set[str] = set()
    for m in REF_PATTERN.finditer(text):
        out.add(m.group(1).lower())
    for m in HARDCODED_PATTERN.finditer(text):
        # DBT_STAGING.STG_OLIDS_X -> stg_olids_x
        out.add(m.group(1).lower())
    return out


def collect_aliases(text: str) -> dict[str, str]:
    """Map alias -> table for each `ref('stg_olids_X') as alias` or
    `DBT_STAGING.STG_OLIDS_X alias` occurrence."""
    out: dict[str, str] = {}
    for pattern in (REF_ALIAS_PATTERN, HARDCODED_ALIAS_PATTERN):
        for m in pattern.finditer(text):
            table = m.group(1).lower()
            alias = m.group(2).lower()
            # If an alias maps to multiple tables across joins, mark ambiguous.
            if alias in out and out[alias] != table:
                out[alias] = "__ambiguous__"
            else:
                out[alias] = table
    return out


@dataclass
class FileReport:
    path: pathlib.Path
    renamed_counts: dict[str, int] = field(default_factory=dict)
    ambiguous_columns: dict[str, set[str]] = field(default_factory=dict)
    removed_hits: dict[str, int] = field(default_factory=dict)
    in_scope_tables: list[str] = field(default_factory=list)


def process_file(
    path: pathlib.Path, table_rules: dict, cross_cutting_keys: set[str], dry_run: bool
) -> FileReport | None:
    text = path.read_text(encoding="utf-8")
    refs = collect_refs(text)
    if not refs:
        return None

    ctx = RenameContext(cross_cutting_keys=set(cross_cutting_keys))
    for ref in refs:
        rules = table_rules.get(ref)
        if not rules or rules.get("delete"):
            continue
        ctx.add_table(ref, rules["renames"], rules["removes"])

    if not ctx.rename_targets and not ctx.removed_cols:
        return None

    aliases = collect_aliases(text)  # alias -> table

    report = FileReport(path=path, in_scope_tables=sorted(ctx.in_scope_tables))
    new_text = text

    # 1) Qualified rewrites: alias.col -> alias.new_col. Driven by per-table
    #    renames, scoped to the specific table the alias resolves to.
    for alias, table in aliases.items():
        if table == "__ambiguous__":
            continue
        per_table = ctx.per_table_renames.get(table) or {}
        for old, new in per_table.items():
            if old == new:
                continue
            pattern = re.compile(rf"\b{re.escape(alias)}\.{re.escape(old)}\b", re.IGNORECASE)
            new_text, n = pattern.subn(f"{alias}.{new}", new_text)
            if n:
                key = f"{alias}.{old} -> {alias}.{new}"
                report.renamed_counts[key] = report.renamed_counts.get(key, 0) + n

    # 2) Unqualified rewrites: only for cross-cutting renames (LDS metadata,
    #    publisher_organisation_code) which apply to every in-scope table. Skip
    #    if the column maps to multiple new names anywhere in scope.
    for old, targets in sorted(ctx.rename_targets.items()):
        if old not in ctx.cross_cutting_keys:
            continue  # table-specific rename — already handled via aliases
        if len(targets) != 1:
            report.ambiguous_columns[old] = targets
            continue
        new = next(iter(targets))
        # Don't rewrite `alias.old` again — qualified pass already handled.
        # The whole-word pattern would match those too. Use negative lookbehind
        # to skip dotted references entirely (they're alias.col patterns).
        pattern = re.compile(rf"(?<![\w.]){re.escape(old)}\b")
        new_text, n = pattern.subn(new, new_text)
        if n:
            key = f"{old} -> {new}"
            report.renamed_counts[key] = report.renamed_counts.get(key, 0) + n

    # 3) Track ambiguous table-specific renames where the file uses unqualified
    #    references. We still want to flag for manual review.
    for old, targets in sorted(ctx.rename_targets.items()):
        if old in ctx.cross_cutting_keys:
            continue
        if len(targets) > 1:
            report.ambiguous_columns[old] = targets

    # 4) Detect removed columns still being referenced (post-rewrite text).
    #    Only flag if NO in-scope table keeps the column (i.e. all in-scope
    #    tables that mentioned it removed it). Otherwise it's almost certainly
    #    referring to a different table's identically-named column.
    tables_keeping_col: dict[str, set[str]] = defaultdict(set)
    for tname in ctx.in_scope_tables:
        renames = ctx.per_table_renames.get(tname) or {}
        removes_for_table = set(table_rules.get(tname, {}).get("removes") or [])
        for col in ctx.removed_cols:
            if col not in removes_for_table and col not in renames:
                tables_keeping_col[col].add(tname)

    for col in sorted(ctx.removed_cols):
        if tables_keeping_col.get(col):
            continue  # at least one in-scope table still has it
        n = len(re.findall(rf"\b{re.escape(col)}\b", new_text))
        if n:
            report.removed_hits[col] = n

    if new_text != text and not dry_run:
        path.write_text(new_text, encoding="utf-8", newline="\n")

    return report


def walk_files(paths: list[pathlib.Path]):
    for root in paths:
        if not root.exists():
            continue
        for ext in ("*.sql", "*.yml", "*.yaml"):
            for p in root.rglob(ext):
                yield p


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Don't write changes")
    ap.add_argument(
        "--paths", nargs="*", type=pathlib.Path,
        help="Override scan paths (default: modelling, reporting, published, semantic, macros)",
    )
    args = ap.parse_args()

    table_rules, cross_cutting_keys = load_rules()
    scan = (
        [(REPO / p).resolve() for p in args.paths]
        if args.paths
        else SCAN_DIRS
    )

    touched: list[FileReport] = []
    ambiguous: list[FileReport] = []
    removed_hits: list[FileReport] = []

    for path in walk_files(scan):
        rep = process_file(path, table_rules, cross_cutting_keys, args.dry_run)
        if rep is None:
            continue
        if rep.renamed_counts:
            touched.append(rep)
        if rep.ambiguous_columns:
            ambiguous.append(rep)
        if rep.removed_hits:
            removed_hits.append(rep)

    print("=== Downstream rewrite summary ===\n")
    print(f"Files with renames applied: {len(touched)}")
    print(f"Files with ambiguous columns (manual review): {len(ambiguous)}")
    print(f"Files referencing removed columns (manual review): {len(removed_hits)}")
    print()

    if touched:
        print("--- Renames applied ---")
        for rep in touched[:50]:
            rel = rep.path.relative_to(REPO)
            cnts = ", ".join(f"{k} ({n})" for k, n in sorted(rep.renamed_counts.items())[:5])
            extra = "" if len(rep.renamed_counts) <= 5 else f" + {len(rep.renamed_counts) - 5} more"
            print(f"  {rel}: {cnts}{extra}")
        if len(touched) > 50:
            print(f"  ... and {len(touched) - 50} more")
        print()

    if ambiguous:
        print("--- AMBIGUOUS columns (skipped, manual review) ---")
        for rep in ambiguous:
            rel = rep.path.relative_to(REPO)
            print(f"  {rel}")
            print(f"    in-scope tables: {', '.join(rep.in_scope_tables)}")
            for col, targets in rep.ambiguous_columns.items():
                print(f"    {col} -> {{{', '.join(sorted(targets))}}}")
        print()

    if removed_hits:
        print("--- REMOVED columns still referenced (manual review) ---")
        for rep in removed_hits:
            rel = rep.path.relative_to(REPO)
            for col, n in rep.removed_hits.items():
                print(f"  {rel}: {col} ({n} hits)")
        print()

    if args.dry_run:
        print("[DRY RUN] No files were written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
