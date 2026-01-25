#!/usr/bin/env python3
"""Backfill model ownership from git history.

For each model YAML file, finds the original author of the corresponding SQL file
and adds config.meta.owner with their name.

Usage:
    python scripts/ownership/backfill_model_owners.py [--dry-run]
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml


def get_all_file_authors() -> dict[str, str]:
    """Get original authors for all files in one git command.

    Returns a dict mapping filename (not full path) to author name.
    Uses filename-only matching since files may have been moved.
    """
    try:
        # Get all SQL files with their first commit author
        result = subprocess.run(
            ['git', 'log', '--diff-filter=A', '--name-only', '--format=%an', '--', 'models/**/*.sql'],
            capture_output=True,
            text=True,
            check=True
        )

        authors = {}
        current_author = None

        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            if line.endswith('.sql'):
                if current_author:
                    # Use filename only since files may have moved
                    filename = line.split('/')[-1]
                    # Always overwrite - git log is newest-first, so last entry is oldest (original author)
                    authors[filename] = current_author
            else:
                current_author = line

        return authors
    except subprocess.CalledProcessError:
        return {}


def find_sql_file(yaml_path: Path, model_name: str) -> Path | None:
    """Find the SQL file for a model."""
    sql_path = yaml_path.parent / f"{model_name}.sql"
    if sql_path.exists():
        return sql_path

    for sql_file in yaml_path.parent.rglob(f"{model_name}.sql"):
        return sql_file

    return None


def process_yaml_file(yaml_path: Path, authors: dict[str, str], dry_run: bool = False) -> list[tuple[str, str]]:
    """Process a single YAML file, returns list of (model_name, owner) tuples that were updated."""
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding='utf-8'))
        if not data or 'models' not in data:
            return []
    except (yaml.YAMLError, Exception):
        return []

    updates = []
    for model in data.get('models', []):
        model_name = model.get('name')
        if not model_name:
            continue

        # Skip if already has owner
        config = model.get('config', {})
        meta = config.get('meta', {}) if config else {}
        if meta and meta.get('owner'):
            continue

        # Find the SQL file
        sql_path = find_sql_file(yaml_path, model_name)
        if not sql_path:
            continue

        # Look up author from pre-built cache (by filename only)
        filename = sql_path.name
        owner_name = authors.get(filename)
        if not owner_name:
            continue

        updates.append((model_name, owner_name))

    # Apply all updates to this YAML file at once
    if updates and not dry_run:
        try:
            from ruamel.yaml import YAML
            from ruamel.yaml.comments import CommentedMap
            ruamel = YAML()
            ruamel.preserve_quotes = True
            ruamel.indent(mapping=2, sequence=4, offset=2)

            with open(yaml_path, 'r', encoding='utf-8') as f:
                data = ruamel.load(f)

            models_to_update = {name for name, _ in updates}
            for model in data.get('models', []):
                if model.get('name') not in models_to_update:
                    continue

                owner_name = next(o for n, o in updates if n == model.get('name'))

                if 'config' not in model:
                    new_config = CommentedMap()
                    new_config['meta'] = CommentedMap([('owner', CommentedMap([('name', owner_name)]))])

                    keys = list(model.keys())
                    insert_after = 'description' if 'description' in keys else 'name'
                    insert_idx = keys.index(insert_after) + 1

                    items = list(model.items())
                    items.insert(insert_idx, ('config', new_config))
                    model.clear()
                    for k, v in items:
                        model[k] = v
                else:
                    if 'meta' not in model['config']:
                        model['config']['meta'] = CommentedMap()
                    model['config']['meta']['owner'] = CommentedMap([('name', owner_name)])

            with open(yaml_path, 'w', encoding='utf-8') as f:
                ruamel.dump(data, f)

        except ImportError:
            print("  Error: ruamel.yaml is required. Install with: pip install ruamel.yaml")
            return []
        except Exception as e:
            print(f"  Error processing {yaml_path}: {e}")
            return []

    return updates


def main() -> int:
    parser = argparse.ArgumentParser(description='Backfill model ownership from git history')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without modifying files')
    args = parser.parse_args()

    root = Path('.')
    models_dir = root / 'models'

    if not models_dir.exists():
        print("No models directory found.")
        return 1

    print("Loading git history...")
    authors = get_all_file_authors()
    print(f"Found authors for {len(authors)} SQL files\n")

    yaml_files = [f for f in list(models_dir.rglob('*.yml')) + list(models_dir.rglob('*.yaml'))
                  if 'dbt_packages' not in str(f)]

    print(f"Processing {len(yaml_files)} YAML files...\n")

    total_updated = 0
    for yaml_path in sorted(yaml_files):
        updates = process_yaml_file(yaml_path, authors, args.dry_run)
        for model_name, owner_name in updates:
            action = "Would add" if args.dry_run else "Added"
            print(f"  {action} owner for {model_name}: {owner_name}")
            total_updated += 1

    print(f"\n{'Would update' if args.dry_run else 'Updated'} {total_updated} models with owner metadata.")

    if args.dry_run and total_updated > 0:
        print("\nRun without --dry-run to apply changes.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
