#!/usr/bin/env python3
"""Set owner metadata for new models in a PR.

Finds models added in the current branch (compared to main) that don't have
owner metadata and adds it based on the PR author.

Usage:
    python scripts/ownership/set_owner_for_new_models.py --author-name "Name"
    python scripts/ownership/set_owner_for_new_models.py --from-git  # Uses git config user.name
"""

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


def get_changed_sql_files(base_branch: str = 'origin/main') -> list[Path]:
    """Get SQL files added or modified in current branch vs base."""
    try:
        result = subprocess.run(
            ['git', 'diff', '--name-only', '--diff-filter=A', base_branch, '--', 'models/*.sql'],
            capture_output=True,
            text=True,
            check=True
        )
        files = [Path(f) for f in result.stdout.strip().split('\n') if f]
        return [f for f in files if f.exists()]
    except subprocess.CalledProcessError:
        return []


def get_git_user() -> dict | None:
    """Get current git user name."""
    try:
        name_result = subprocess.run(
            ['git', 'config', 'user.name'],
            capture_output=True, text=True, check=True
        )
        name = name_result.stdout.strip()
        if name:
            return {'name': name}
    except subprocess.CalledProcessError:
        pass
    return None


def find_yaml_for_model(sql_path: Path) -> Path | None:
    """Find the YAML file that should contain this model's definition."""
    model_name = sql_path.stem
    model_dir = sql_path.parent

    # Check for model-specific YAML
    yaml_path = model_dir / f"{model_name}.yml"
    if yaml_path.exists():
        return yaml_path

    # Check for schema.yml or similar in same directory
    for pattern in ['*.yml', '*.yaml']:
        for yaml_file in model_dir.glob(pattern):
            try:
                data = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
                if data and 'models' in data:
                    for model in data['models']:
                        if model.get('name') == model_name:
                            return yaml_file
            except (yaml.YAMLError, Exception):
                continue

    return None


def model_has_owner(yaml_path: Path, model_name: str) -> bool:
    """Check if model already has owner metadata."""
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding='utf-8'))
        if not data or 'models' not in data:
            return False

        for model in data.get('models', []):
            if model.get('name') == model_name:
                return bool(model.get('meta', {}).get('owner'))
    except (yaml.YAMLError, Exception):
        pass
    return False


def add_owner_to_yaml(yaml_path: Path, model_name: str, owner: dict) -> bool:
    """Add owner metadata to a model in a YAML file."""
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
                if 'owner' not in model['meta']:
                    model['meta']['owner'] = owner
                    modified = True
                break

        if modified:
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
        print(f"Error updating {yaml_path}: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description='Set owner for new models in PR')
    parser.add_argument('--author-name', help='Author name for ownership')
    parser.add_argument('--from-git', action='store_true', help='Use git config for author')
    parser.add_argument('--base-branch', default='origin/main', help='Base branch to compare against')
    args = parser.parse_args()

    # Determine owner
    if args.from_git:
        owner = get_git_user()
        if not owner:
            print("Error: Could not get git user config")
            return 1
    elif args.author_name:
        owner = {'name': args.author_name}
    else:
        print("Error: Provide --author-name or use --from-git")
        return 1

    print(f"Setting owner: {owner['name']}\n")

    # Find new SQL files
    new_sql_files = get_changed_sql_files(args.base_branch)
    if not new_sql_files:
        print("No new model SQL files found in this branch.")
        return 0

    print(f"Found {len(new_sql_files)} new SQL files\n")

    updated = 0
    for sql_path in new_sql_files:
        model_name = sql_path.stem
        yaml_path = find_yaml_for_model(sql_path)

        if not yaml_path:
            print(f"  Warning: No YAML file found for {model_name}")
            continue

        if model_has_owner(yaml_path, model_name):
            print(f"  Skipping {model_name}: already has owner")
            continue

        if add_owner_to_yaml(yaml_path, model_name, owner):
            print(f"  Added owner to {model_name}")
            updated += 1

    print(f"\nUpdated {updated} models with owner metadata.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
