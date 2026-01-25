#!/usr/bin/env python3
"""Backfill model ownership from git history.

For each model YAML file, finds the original author of the corresponding SQL file
and adds meta.owner with their name.

Usage:
    python scripts/ownership/backfill_model_owners.py [--dry-run]
"""

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


def get_original_author(file_path: Path) -> dict | None:
    """Get the original author of a file from git history."""
    try:
        # --diff-filter=A shows only the commit that added the file
        # --follow tracks renames
        result = subprocess.run(
            ['git', 'log', '--diff-filter=A', '--follow', '--format=%an', '--', str(file_path)],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout.strip()
        if output:
            # Take the last line (oldest commit that added the file)
            lines = output.strip().split('\n')
            name = lines[-1].strip() if lines else None
            if name:
                return {'name': name}
    except subprocess.CalledProcessError:
        pass
    return None


def find_sql_file(yaml_path: Path, model_name: str) -> Path | None:
    """Find the SQL file for a model."""
    # Check same directory first
    sql_path = yaml_path.parent / f"{model_name}.sql"
    if sql_path.exists():
        return sql_path

    # Check subdirectories
    for sql_file in yaml_path.parent.rglob(f"{model_name}.sql"):
        return sql_file

    return None


def update_yaml_with_owner(yaml_path: Path, model_name: str, owner: dict, dry_run: bool = False) -> bool:
    """Update a YAML file to add owner metadata for a model."""
    try:
        content = yaml_path.read_text(encoding='utf-8')
        data = yaml.safe_load(content)

        if not data or 'models' not in data:
            return False

        modified = False
        for model in data.get('models', []):
            if model.get('name') == model_name:
                if 'meta' not in model:
                    model['meta'] = {}

                # Skip if owner already set
                if 'owner' in model['meta']:
                    return False

                model['meta']['owner'] = owner
                modified = True
                break

        if modified and not dry_run:
            # Use ruamel.yaml to preserve formatting if available, otherwise standard yaml
            try:
                from ruamel.yaml import YAML
                ruamel = YAML()
                ruamel.preserve_quotes = True
                ruamel.indent(mapping=2, sequence=4, offset=2)
                with open(yaml_path, 'w', encoding='utf-8') as f:
                    ruamel.dump(data, f)
            except ImportError:
                with open(yaml_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        return modified

    except (yaml.YAMLError, Exception) as e:
        print(f"  Error processing {yaml_path}: {e}")
        return False


def process_yaml_file(yaml_path: Path, dry_run: bool = False) -> int:
    """Process a single YAML file, updating owner for all models."""
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding='utf-8'))
        if not data or 'models' not in data:
            return 0
    except (yaml.YAMLError, Exception):
        return 0

    updated = 0
    for model in data.get('models', []):
        model_name = model.get('name')
        if not model_name:
            continue

        # Skip if already has owner
        if model.get('meta', {}).get('owner'):
            continue

        # Find the SQL file
        sql_path = find_sql_file(yaml_path, model_name)
        if not sql_path:
            print(f"  Warning: No SQL file found for {model_name}")
            continue

        # Get the original author
        owner = get_original_author(sql_path)
        if not owner:
            print(f"  Warning: Could not determine author for {sql_path}")
            continue

        # Update the YAML
        if update_yaml_with_owner(yaml_path, model_name, owner, dry_run):
            action = "Would add" if dry_run else "Added"
            print(f"  {action} owner for {model_name}: {owner['name']}")
            updated += 1

    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description='Backfill model ownership from git history')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without modifying files')
    args = parser.parse_args()

    root = Path('.')
    models_dir = root / 'models'

    if not models_dir.exists():
        print("No models directory found.")
        return 1

    total_updated = 0
    yaml_files = list(models_dir.rglob('*.yml')) + list(models_dir.rglob('*.yaml'))

    print(f"Scanning {len(yaml_files)} YAML files...\n")

    for yaml_path in sorted(yaml_files):
        if 'dbt_packages' in str(yaml_path):
            continue

        updated = process_yaml_file(yaml_path, args.dry_run)
        if updated:
            total_updated += updated

    print(f"\n{'Would update' if args.dry_run else 'Updated'} {total_updated} models with owner metadata.")

    if args.dry_run and total_updated > 0:
        print("\nRun without --dry-run to apply changes.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
