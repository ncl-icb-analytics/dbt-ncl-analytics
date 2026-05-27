"""Transitive rewrite pass for OLIDS 2026 column renames.

The initial `rewrite_downstream.py` only touches files that `ref('stg_olids_*')`
or `ref('raw_olids_*')` directly. Many downstream models consume OLIDS columns
indirectly through intermediate / dim / fct models — e.g. `fct_gp_appointment_costs`
selects from `int_appointment_gp_clean_recent`, which selects from
`int_appointment_gp_clean`, which selects from `stg_olids_appointment`.

This pass walks the ref dependency graph and applies cross-cutting OLIDS column
renames to every file in the transitive closure of stg_olids_* consumers.
Conservative: only applies unambiguous, OLIDS-specific column names that won't
collide with non-OLIDS code (lds_record_id, record_owner_organisation_code,
appointment_status_concept_id, date_time_booked, is_dummy_patient, etc.).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
from collections import defaultdict

import yaml

REPO = pathlib.Path(__file__).resolve().parents[3]
RENAMES_FILE = pathlib.Path(__file__).with_name("column_renames.yml")
SCAN_DIRS = [
    REPO / "models",
    REPO / "macros",
    REPO / "analyses",   # exploratory queries / DQ reports that ref staging
    REPO / "tests",      # custom data tests
    REPO / "snapshots",  # rare, but possible
]
# Files matching `ref('NAME')` — captures NAME.
REF_PATTERN = re.compile(r"""\{\{\s*ref\(\s*['"]([\w]+)['"]\s*\)\s*\}\}""", re.IGNORECASE)

# Files to skip in the transitive pass:
#  - models/raw/ regenerates automatically from sources in Phase 4
#  - stg_olids_ndoo_hashed / stg_olids_postcode_hash / stg_olids_appointment_practitioner
#    are unchanged at source per issue #747
EXCLUDE_DIRS = [
    REPO / "models" / "raw",
]
EXCLUDE_FILE_STEMS = {
    "stg_olids_ndoo_hashed",
    "stg_olids_postcode_hash",
    "stg_olids_appointment_practitioner",
}

# Records any file the codemod couldn't open (permissions, encoding, missing).
# Surfaced at the end and used to exit non-zero so a partial run isn't silent.
READ_FAILURES: list[tuple[pathlib.Path, str]] = []


def load_safe_renames():
    """Return (rename_map, remove_set, drop_set).

    rename_map: column -> new_name for columns that are OLIDS-specific enough
                to safely rewrite anywhere in the OLIDS subgraph without
                collision risk.
    Ambiguous renames (organisation_id, type, result_value_units_concept_id)
    are intentionally excluded — they were handled by the alias-aware pass.
    """
    data = yaml.safe_load(RENAMES_FILE.read_text())
    defaults = data.get("defaults", {}).get("renames", {}) or {}
    defaults_removes = data.get("defaults", {}).get("removes", []) or []

    # Track per-target → which sources map to it, to detect intra-OLIDS collisions
    per_target = defaultdict(set)
    raw_renames: dict[str, str] = dict(defaults)
    raw_removes: set[str] = set(defaults_removes)

    # Columns that are too generic — skip transitive rewrite for these
    BLOCKLIST = {
        "id",
        "type",
        "type_code",
        "type_desc",
        "organisation_id",
        "practitioner_id",
        "patient_id",
        "person_id",
        "encounter_id",
        "last_name",
        "result_value_units_concept_id",  # different new names per table
        "location",  # only renamed in stg_olids_schedule; too generic elsewhere
    }

    for name, cfg in (data.get("tables") or {}).items():
        cfg = cfg or {}
        for old, new in (cfg.get("overrides") or {}).items():
            if old == new:
                raw_renames.pop(old, None)
        for old, new in (cfg.get("renames") or {}).items():
            if old in raw_renames and raw_renames[old] != new:
                # collision within OLIDS — table-specific rename diverges
                raw_renames[old] = "__ambiguous__"
            else:
                raw_renames[old] = new
            per_target[old].add(new)
        for col in cfg.get("removes") or []:
            if col:
                raw_removes.add(col)

    # Strip blocklist and ambiguous entries
    safe = {
        old: new
        for old, new in raw_renames.items()
        if old not in BLOCKLIST and new != "__ambiguous__"
    }
    return safe, raw_removes


def build_ref_graph(scan_paths: list[pathlib.Path]) -> tuple[dict[str, pathlib.Path], dict[pathlib.Path, set[str]]]:
    """Return (name_to_path, path_to_refs).

    name_to_path maps a model/macro file's basename (without extension) to its path.
    path_to_refs maps a file path to the set of names it ref()s.
    """
    name_to_path: dict[str, pathlib.Path] = {}
    path_to_refs: dict[pathlib.Path, set[str]] = {}
    for root in scan_paths:
        if not root.exists():
            continue
        for p in root.rglob("*.sql"):
            stem = p.stem
            name_to_path.setdefault(stem, p)
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                READ_FAILURES.append((p, str(exc)))
                continue
            refs = {m.group(1).lower() for m in REF_PATTERN.finditer(text)}
            path_to_refs[p] = refs
    return name_to_path, path_to_refs


def discover_olids_subgraph(
    name_to_path: dict[str, pathlib.Path], path_to_refs: dict[pathlib.Path, set[str]]
) -> set[pathlib.Path]:
    """Return all files in the transitive closure of consumers of stg_olids_* / raw_olids_*.

    We work backwards from leaf OLIDS-touched models: any file whose `ref()` graph
    reaches a stg_olids_*/raw_olids_* model is considered OLIDS-touched.
    """
    # Seed: every file that directly refs stg_olids_* or raw_olids_*
    seeds: set[pathlib.Path] = set()
    for path, refs in path_to_refs.items():
        if any(r.startswith("stg_olids_") or r.startswith("raw_olids_") for r in refs):
            seeds.add(path)

    # Add the stg/raw files themselves so consumers of them resolve
    olids_set: set[pathlib.Path] = set(seeds)
    for name, path in name_to_path.items():
        if name.startswith("stg_olids_") or name.startswith("raw_olids_"):
            olids_set.add(path)

    # Expand: any file that refs a model already in olids_set gets added.
    # Iterate to fixed point.
    changed = True
    while changed:
        changed = False
        for path, refs in path_to_refs.items():
            if path in olids_set:
                continue
            for ref in refs:
                ref_path = name_to_path.get(ref)
                if ref_path is not None and ref_path in olids_set:
                    olids_set.add(path)
                    changed = True
                    break
    return olids_set


def yml_pair(sql_path: pathlib.Path) -> pathlib.Path | None:
    candidate = sql_path.with_suffix(".yml")
    return candidate if candidate.exists() else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    safe_renames, removes = load_safe_renames()
    print(f"Safe transitive renames loaded: {len(safe_renames)}")

    name_to_path, path_to_refs = build_ref_graph(SCAN_DIRS)
    print(f"Indexed {len(name_to_path):,} model/macro files")

    olids_subgraph = discover_olids_subgraph(name_to_path, path_to_refs)
    print(f"OLIDS subgraph: {len(olids_subgraph):,} files")

    # Also include the .yml siblings of every .sql in the subgraph
    files_to_process: set[pathlib.Path] = set()
    for sql in olids_subgraph:
        if any(str(sql).startswith(str(ex)) for ex in EXCLUDE_DIRS):
            continue
        if sql.stem in EXCLUDE_FILE_STEMS:
            continue
        files_to_process.add(sql)
        yml = yml_pair(sql)
        if yml is not None:
            files_to_process.add(yml)

    # Compile pattern per rename. Unlike the first downstream pass, the
    # transitive pass does NOT use a negative lookbehind for `.` — we want
    # to rewrite both standalone `planned_duration` and qualified
    # `appt.planned_duration` references. The OLIDS-specific column names
    # in `safe_renames` don't appear in non-OLIDS code, so dotted matches
    # are safe.
    patterns = [(re.compile(rf"(?<!\w){re.escape(old)}\b"), old, new) for old, new in safe_renames.items()]

    touched_counts: dict[pathlib.Path, dict[str, int]] = defaultdict(dict)
    removed_hits: dict[pathlib.Path, dict[str, int]] = defaultdict(dict)

    for path in sorted(files_to_process):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            READ_FAILURES.append((path, str(exc)))
            continue
        new_text = text
        for pat, old, new in patterns:
            new_text, n = pat.subn(new, new_text)
            if n:
                key = f"{old} -> {new}"
                touched_counts[path][key] = touched_counts[path].get(key, 0) + n

        # Only flag removed cols that aren't generic English words / YAML keys.
        # `description`, `id`, etc. cause too many false positives in YAML.
        NOISY_REMOVES = {"description", "id", "practitioner_id", "lds_end_date_time"}
        for col in removes:
            if col in NOISY_REMOVES:
                continue
            n = len(re.findall(rf"(?<!\w){re.escape(col)}\b", new_text))
            if n:
                removed_hits[path][col] = n

        if new_text != text and not args.dry_run:
            path.write_text(new_text, encoding="utf-8", newline="\n")

    print(f"\nFiles rewritten: {sum(1 for r in touched_counts.values() if r)}")
    print(f"Files with removed-col refs (manual review): {sum(1 for r in removed_hits.values() if r)}")

    if touched_counts:
        print("\n--- Renames applied ---")
        for path, counts in sorted(touched_counts.items()):
            if not counts:
                continue
            rel = path.relative_to(REPO)
            cnts = ", ".join(f"{k} ({n})" for k, n in sorted(counts.items())[:4])
            extra = "" if len(counts) <= 4 else f" + {len(counts) - 4} more"
            print(f"  {rel}: {cnts}{extra}")

    if removed_hits:
        print("\n--- Removed cols still referenced ---")
        for path, hits in sorted(removed_hits.items()):
            if not hits:
                continue
            rel = path.relative_to(REPO)
            for col, n in hits.items():
                print(f"  {rel}: {col} ({n})")

    if args.dry_run:
        print("\n[DRY RUN] No files written.")

    if READ_FAILURES:
        print(f"\n--- File read failures ({len(READ_FAILURES)}) ---", file=sys.stderr)
        for path, err in READ_FAILURES:
            print(f"  {path}: {err}", file=sys.stderr)
        print(
            "\nExiting non-zero because some files could not be read — the "
            "rewrite is partial. Re-run after fixing the underlying issue.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
