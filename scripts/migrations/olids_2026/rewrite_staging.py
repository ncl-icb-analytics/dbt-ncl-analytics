"""Rewrite stg_olids_*.sql and .yml using column_renames.yml.

Conservative line-oriented codemod that exploits the regular shape of the
existing staging models (one column per line in the SELECT list, one
`- name: foo` per line in the schema yml). Idempotent and safe to re-run.

For each staging model:
  - rename:        replace `old_col,` with `new_col,` in SQL; replace
                   `- name: old_col` with `- name: new_col` in YAML.
  - remove:        delete the matching SQL line and YAML column block.
  - type_cast:     replace `old_col,` with the cast expression (which still
                   yields `... AS old_col`-or-new alias as configured).
  - delete (file): remove both .sql and .yml.
  - overrides:     skip the default rename for the listed columns.

Unhandled columns (in SELECT but not in any rule) pass through untouched.
A summary report is printed at the end.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
from dataclasses import dataclass, field
from typing import Optional

import yaml

REPO = pathlib.Path(__file__).resolve().parents[3]
STAGING_DIR = REPO / "models" / "staging" / "olids"
RENAMES_FILE = pathlib.Path(__file__).with_name("column_renames.yml")


@dataclass
class TableRules:
    name: str
    raw: str
    renames: dict[str, str] = field(default_factory=dict)
    removes: list[str] = field(default_factory=list)
    type_casts: dict[str, str] = field(default_factory=dict)
    overrides: dict[str, str] = field(default_factory=dict)
    # additions[col] = description (empty string if no description supplied).
    # Supports both forms in YAML: a bare list of strings, or a list of
    # `{name, description}` dicts.
    additions: dict[str, str] = field(default_factory=dict)
    delete: bool = False
    delete_reason: Optional[str] = None
    unchanged: bool = False
    needs_manual_review: list[str] = field(default_factory=list)


def _parse_additions(raw) -> dict[str, str]:
    """Accept either ['col1', 'col2'] or [{'name': 'col1', 'description': '...'}]."""
    out: dict[str, str] = {}
    if not raw:
        return out
    for item in raw:
        if isinstance(item, str):
            out[item] = ""
        elif isinstance(item, dict) and "name" in item:
            out[item["name"]] = item.get("description") or ""
    return out


def load_rules() -> dict[str, TableRules]:
    data = yaml.safe_load(RENAMES_FILE.read_text())
    defaults_renames = data.get("defaults", {}).get("renames", {}) or {}
    defaults_removes = data.get("defaults", {}).get("removes", []) or []

    out: dict[str, TableRules] = {}
    for name, raw_cfg in (data.get("tables") or {}).items():
        cfg = raw_cfg or {}
        renames = dict(defaults_renames)
        # Per-table overrides remove default renames where the source kept the old name.
        for k, v in (cfg.get("overrides") or {}).items():
            if k == v:
                renames.pop(k, None)
            else:
                renames[k] = v
        # Per-table renames take precedence
        for k, v in (cfg.get("renames") or {}).items():
            renames[k] = v
        removes = list(defaults_removes) + [
            r for r in (cfg.get("removes") or []) if r  # filter empty list literal
        ]
        out[name] = TableRules(
            name=name,
            raw=cfg.get("raw", ""),
            renames=renames,
            removes=removes,
            type_casts=cfg.get("type_casts") or {},
            overrides=cfg.get("overrides") or {},
            additions=_parse_additions(cfg.get("additions")),
            delete=bool(cfg.get("delete", False)),
            delete_reason=cfg.get("delete_reason"),
            unchanged=bool(cfg.get("unchanged", False)),
            needs_manual_review=cfg.get("needs_manual_review") or [],
        )
    return out


# Match a SELECT-list bare identifier line, ignoring leading whitespace and
# allowing optional trailing comma. Captures the identifier.
SQL_COL_LINE = re.compile(
    r"^(\s*)([a-z_][a-z0-9_]*)\s*(,?)\s*(--.*)?$", re.IGNORECASE
)
# Match `<expr> AS alias` style (e.g. cast outputs). Match the alias.
SQL_COL_AS_LINE = re.compile(
    r"^(\s*)(.+?\s+as\s+)([a-z_][a-z0-9_]*)\s*(,?)\s*(--.*)?$",
    re.IGNORECASE,
)

YML_NAME_LINE = re.compile(r"^(\s*)(-\s*name:\s*)([a-z_][a-z0-9_]*)\s*$", re.IGNORECASE)


@dataclass
class FileReport:
    path: pathlib.Path
    renamed: list[tuple[str, str]] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    cast: list[str] = field(default_factory=list)
    additions: list[str] = field(default_factory=list)
    unchanged_cols: list[str] = field(default_factory=list)
    deleted_file: bool = False
    needs_manual: list[str] = field(default_factory=list)


def rewrite_sql(text: str, rules: TableRules, report: FileReport) -> str:
    """Rewrite a stg SQL: rename/remove/type_cast on SELECT list lines.

    Operates on the SELECT list line-by-line. After the SELECT list ends,
    the remaining clauses (WHERE / QUALIFY / ORDER BY) get a whole-word
    rename pass so that references like `order by lds_start_date_time desc`
    track the renames.
    """
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    in_select = False
    seen_from = False
    additions_emitted = False
    emitted_select_cols: list[str] = []

    def _post_select_rewrite(line: str) -> str:
        rewritten = line
        for old, new in rules.renames.items():
            if old == new:
                continue
            rewritten = re.sub(rf"\b{re.escape(old)}\b", new, rewritten)
        return rewritten

    for line in lines:
        stripped = line.strip().lower()
        if not in_select and stripped.startswith("select"):
            in_select = True
            out.append(line)
            continue
        if not in_select and not seen_from:
            # Pre-SELECT block (config, jinja, comments) — apply identifier rewrite
            # to catch things like `cluster_by=['source_code_id']`.
            out.append(_post_select_rewrite(line))
            continue
        if in_select and not seen_from and re.match(r"\s*from\b", line, re.IGNORECASE):
            # About to leave SELECT list. Emit additions before FROM as real SELECT
            # columns so downstream consumers can reference them. Adds a trailing
            # comma to the prior last column. Idempotent: skip any addition
            # already present in the existing SELECT.
            additions_to_emit = [
                c for c in (rules.additions or []) if c not in emitted_select_cols
            ]
            if additions_to_emit and not additions_emitted:
                indent = "    "
                # Find the last non-blank line in `out` that's a SELECT-list column
                # (i.e. doesn't start with `--`) and add a trailing comma if missing.
                for i in range(len(out) - 1, -1, -1):
                    candidate = out[i].rstrip()
                    if not candidate.strip():
                        continue
                    if candidate.lstrip().startswith("--"):
                        continue
                    if not candidate.endswith(","):
                        out[i] = candidate + ","
                    break
                out.append("")
                out.append(f"{indent}-- New columns exposed by the 2026 OLIDS schema realignment (issue #747)")
                for j, col in enumerate(additions_to_emit):
                    comma = "," if j < len(additions_to_emit) - 1 else ""
                    out.append(f"{indent}{col}{comma}")
                additions_emitted = True
                report.additions.extend(additions_to_emit)
                emitted_select_cols.extend(additions_to_emit)
            seen_from = True
            in_select = False
            out.append(line)
            continue

        if not in_select:
            out.append(_post_select_rewrite(line) if seen_from else line)
            continue

        # Inside SELECT list — try to recognise a column line
        m_simple = SQL_COL_LINE.match(line)
        m_as = SQL_COL_AS_LINE.match(line)

        col: Optional[str] = None
        is_alias_line = False
        if m_as:
            col = m_as.group(3).lower()
            is_alias_line = True
        elif m_simple:
            col = m_simple.group(2).lower()
            # Skip lines that are SQL keywords we don't want to touch
            if col in {"select", "from", "where", "qualify", "and", "or"}:
                out.append(line)
                continue

        if col is None:
            out.append(line)
            continue

        # Apply rules in priority order: type_cast > rename > remove > keep
        if col in rules.type_casts:
            indent = (m_as.group(1) if is_alias_line else m_simple.group(1))
            comma = (m_as.group(4) if is_alias_line else m_simple.group(3)) or ","
            cast_expr = rules.type_casts[col]
            out.append(f"{indent}{cast_expr}{comma}")
            emitted_select_cols.append(col)
            report.cast.append(col)
            continue

        if col in rules.removes:
            report.removed.append(col)
            # Drop the entire line.
            continue

        if col in rules.renames:
            new = rules.renames[col]
            if new in emitted_select_cols:
                # Source kept both old + new under different names; the renamed
                # output would collide. Drop this line.
                report.removed.append(f"{col} (collision with existing {new})")
                continue
            if is_alias_line:
                indent = m_as.group(1)
                expr = m_as.group(2)  # `... AS `
                comma = m_as.group(4) or ""
                trailing = m_as.group(5) or ""
                # Rename the alias only
                out.append(f"{indent}{expr}{new}{comma} {trailing}".rstrip())
            else:
                indent = m_simple.group(1)
                comma = m_simple.group(3) or ""
                trailing = m_simple.group(4) or ""
                out.append(f"{indent}{new}{comma} {trailing}".rstrip())
            emitted_select_cols.append(new)
            report.renamed.append((col, new))
            continue

        # No rule matched — pass through, record for the unhandled report.
        if col in emitted_select_cols:
            # Pre-existing duplicate (e.g. `lds_registrar_event_id` already in
            # the old SELECT and now also produced by renaming `registrar_event_id`).
            report.removed.append(f"{col} (duplicate)")
            continue
        out.append(line)
        emitted_select_cols.append(col)
        if col not in {"id", "person_id", "patient_id", "encounter_id",
                       "practitioner_id", "lds_is_deleted"}:
            report.unchanged_cols.append(col)

    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def _rewrite_yml_text(line: str, rules: TableRules) -> str:
    """Rewrite renamed identifiers inside structured YAML reference keys.

    Scoped narrowly to lines that carry a single column identifier as their
    value:
      - `field: <identifier>` (relationships test argument)
      - `column_name: <identifier>` / `column: <identifier>` (other tests)
      - `- <identifier>` items under `combination_of_columns:`-style lists

    Free-text descriptions are NOT rewritten — they often contain rename audit
    notes (e.g. "post-2026 OLIDS rename — was record_owner_organisation_code")
    and a mechanical rewrite would eat those references on every rerun. Prose
    drift is handled by an explicit manual sweep when needed.
    """
    if YML_NAME_LINE.match(line):
        return line
    # Match `<indent><key>: <identifier>` where key signals a column reference
    m = re.match(r"^(\s*)(field|column|column_name):\s*([a-z_][a-z0-9_]*)\s*$", line, re.IGNORECASE)
    if m:
        indent, key, ident = m.group(1), m.group(2), m.group(3).lower()
        for old, new in rules.renames.items():
            if old == new:
                continue
            if ident == old:
                return f"{indent}{key}: {new}"
        return line
    # Match `<indent>- <identifier>` items inside a sequence — typically used
    # under `combination_of_columns:`. Conservative: only rewrites when the
    # whole value is a bare identifier (no quotes, no expression).
    m = re.match(r"^(\s*-\s*)([a-z_][a-z0-9_]*)\s*$", line, re.IGNORECASE)
    if m:
        indent, ident = m.group(1), m.group(2).lower()
        for old, new in rules.renames.items():
            if old == new:
                continue
            if ident == old:
                return f"{indent}{new}"
    return line


_PLACEHOLDER_DESCRIPTION_RE = re.compile(
    r"^(\s*)description:\s*New column exposed by the 2026 OLIDS schema realignment.*"
    r"Manual review recommended.*$"
)


def rewrite_yml(text: str, rules: TableRules, report: FileReport) -> str:
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    skip_next_col_block = False
    skip_indent: Optional[int] = None
    last_column_indent: Optional[int] = None  # leading-whitespace col of last kept `- name:`
    # Track the most recently emitted `- name:` so we can backfill the
    # description when we hit a stale placeholder line for an addition that
    # has a real description in column_renames.yml.
    last_addition_name: Optional[str] = None

    for line in lines:
        # If we're inside a removed column's block, skip continuation lines
        # (anything indented further than the `- name:` line).
        if skip_next_col_block:
            stripped = line.lstrip(" ")
            indent = len(line) - len(stripped)
            if not stripped or indent > (skip_indent or 0):
                continue
            skip_next_col_block = False
            skip_indent = None

        m = YML_NAME_LINE.match(line)
        if not m:
            # Replace stale placeholder descriptions with the canonical
            # description from column_renames.yml when one exists.
            pm = _PLACEHOLDER_DESCRIPTION_RE.match(line)
            if pm and last_addition_name and rules.additions.get(last_addition_name):
                indent = pm.group(1)
                desc = rules.additions[last_addition_name]
                escaped = desc.replace("\\", "\\\\").replace('"', '\\"')
                out.append(f'{indent}description: "{escaped}"')
                last_addition_name = None
                continue
            out.append(_rewrite_yml_text(line, rules))
            continue

        leading = m.group(1)  # whitespace before `-`
        prefix = m.group(2)   # `- name: `
        col = m.group(3).lower()
        list_item_indent = len(leading)  # column position of the `-`

        if col in rules.removes:
            report.removed.append(col)
            # Skip this line and any continuation lines indented past the `-`.
            skip_next_col_block = True
            skip_indent = list_item_indent
            continue

        if col in rules.renames:
            new = rules.renames[col]
            out.append(f"{leading}{prefix}{new}")
            last_column_indent = list_item_indent
            last_addition_name = new if new in rules.additions else None
            report.renamed.append((col, new))
            continue

        out.append(line)
        last_column_indent = list_item_indent
        last_addition_name = col if col in rules.additions else None

    # Append additions as bare `- name:` entries at the end of the columns
    # block so downstream documentation matches the SQL SELECT additions.
    # Idempotent: scan the rendered output for existing `- name: <col>` lines
    # and skip additions already declared.
    if rules.additions and last_column_indent is not None:
        existing_names: set[str] = set()
        for ln in out:
            mm = YML_NAME_LINE.match(ln)
            if mm:
                existing_names.add(mm.group(3).lower())
        # Find the right insertion point: just before the table-level `tests:`
        # / `data_tests:` / `config:` key (or end of file). Walk backwards.
        insert_at = len(out)
        for i in range(len(out) - 1, -1, -1):
            s = out[i].lstrip(" ")
            indent = len(out[i]) - len(s)
            if not s:
                continue
            if indent <= last_column_indent - 2 and (
                s.startswith("tests:")
                or s.startswith("data_tests:")
                or s.startswith("config:")
            ):
                insert_at = i
                break
        leading = " " * last_column_indent
        addition_lines = []
        appended: list[str] = []
        for col, desc in rules.additions.items():
            if col in {old for old in rules.removes}:
                continue
            if col.lower() in existing_names:
                continue  # already declared in YAML
            addition_lines.append(f"{leading}- name: {col}")
            if desc:
                # Quote with double quotes so embedded apostrophes survive YAML
                escaped = desc.replace("\\", "\\\\").replace('"', '\\"')
                addition_lines.append(f'{leading}  description: "{escaped}"')
            # Else: emit the column with no description rather than a fake
            # placeholder. Reviewers see an incomplete metadata block and can
            # backfill once the source is live and semantics are clearer.
            appended.append(col)
        if addition_lines:
            out = out[:insert_at] + addition_lines + out[insert_at:]
            report.additions.extend(appended)

    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def process_table(rules: TableRules, dry_run: bool) -> FileReport:
    sql_path = STAGING_DIR / f"{rules.name}.sql"
    yml_path = STAGING_DIR / f"{rules.name}.yml"
    report = FileReport(path=sql_path)

    if rules.delete:
        report.deleted_file = True
        if not dry_run:
            for p in (sql_path, yml_path):
                if p.exists():
                    p.unlink()
        return report

    if rules.unchanged:
        return report

    if not sql_path.exists():
        print(f"  WARN: {sql_path.name} not found", file=sys.stderr)
        return report

    if rules.needs_manual_review:
        report.needs_manual.extend(rules.needs_manual_review)

    sql_text = sql_path.read_text()
    new_sql = rewrite_sql(sql_text, rules, report)
    if new_sql != sql_text and not dry_run:
        sql_path.write_text(new_sql)

    if yml_path.exists():
        # Separate report for YML changes so we don't double-count
        yml_report = FileReport(path=yml_path)
        yml_text = yml_path.read_text()
        new_yml = rewrite_yml(yml_text, rules, yml_report)
        if new_yml != yml_text and not dry_run:
            yml_path.write_text(new_yml)

    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Don't write changes")
    ap.add_argument("--only", nargs="*", help="Process only these stg_olids_* models")
    args = ap.parse_args()

    rules = load_rules()
    reports: list[FileReport] = []

    for name, table_rules in sorted(rules.items()):
        if args.only and name not in args.only:
            continue
        rep = process_table(table_rules, args.dry_run)
        reports.append(rep)

    print("=== Staging rewrite summary ===\n")
    for rep in reports:
        if rep.deleted_file:
            print(f"DELETED  {rep.path.name}")
            continue
        if not (rep.renamed or rep.removed or rep.cast or rep.additions or rep.needs_manual):
            continue
        print(f"\n{rep.path.name}")
        for old, new in rep.renamed:
            print(f"  rename : {old:<45} -> {new}")
        for col in rep.removed:
            print(f"  remove : {col}")
        for col in rep.cast:
            print(f"  cast   : {col}")
        for col in rep.additions:
            print(f"  add    : {col}  (TODO placeholder emitted)")
        for note in rep.needs_manual:
            print(f"  MANUAL : {note}")

    unhandled = sorted({c for r in reports for c in r.unchanged_cols})
    if unhandled:
        print("\nUnhandled columns (pass-through, may want review):")
        for c in unhandled:
            print(f"  - {c}")

    if args.dry_run:
        print("\n[DRY RUN] No files were written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
