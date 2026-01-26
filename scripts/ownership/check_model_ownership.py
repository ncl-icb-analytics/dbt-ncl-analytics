#!/usr/bin/env python3
"""Check for missing model ownership in new models.

Outputs suggestions as JSON for GitHub Actions to post as PR comments.

Usage:
    python scripts/ownership/check_model_ownership.py --author-name "username" --base-branch "origin/main" --output suggestions.json
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml


def get_new_sql_files(base_branch: str) -> list[Path]:
    """Get list of new SQL files in models/ compared to base branch."""
    try:
        result = subprocess.run(
            ['git', 'diff', '--name-only', '--diff-filter=A', base_branch, '--', 'models/**/*.sql'],
            capture_output=True,
            text=True,
            check=True
        )
        files = [Path(f.strip()) for f in result.stdout.strip().split('\n') if f.strip()]
        return [f for f in files if f.exists() and not str(f).startswith('models/raw/')]
    except subprocess.CalledProcessError:
        return []


def find_yaml_for_model(sql_path: Path) -> Path | None:
    """Find the YAML file that defines a model."""
    model_name = sql_path.stem

    # Check for model-specific YAML
    yaml_path = sql_path.with_suffix('.yml')
    if yaml_path.exists():
        return yaml_path

    yaml_path = sql_path.with_suffix('.yaml')
    if yaml_path.exists():
        return yaml_path

    # Check for directory-level YAML files
    for yaml_file in sql_path.parent.glob('*.yml'):
        try:
            data = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
            if data and 'models' in data:
                for model in data['models']:
                    if model.get('name') == model_name:
                        return yaml_file
        except (yaml.YAMLError, Exception):
            continue

    for yaml_file in sql_path.parent.glob('*.yaml'):
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
    """Check if model already has owner metadata in config.meta.owner."""
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding='utf-8'))
        if not data or 'models' not in data:
            return False

        for model in data.get('models', []):
            if model.get('name') == model_name:
                config = model.get('config', {})
                meta = config.get('meta', {}) if config else {}
                return bool(meta and meta.get('owner'))
    except (yaml.YAMLError, Exception):
        pass
    return False


def find_insertion_line(yaml_path: Path, model_name: str) -> tuple[int, str, str]:
    """Find the line number where config should be inserted.

    Returns (line_number, indent, original_line) - original_line must be preserved
    since GitHub suggestions replace rather than insert.
    """
    lines = yaml_path.read_text(encoding='utf-8').split('\n')

    in_target_model = False
    model_indent = 0
    description_line = None
    description_content = None
    name_line = None
    name_content = None

    for i, line in enumerate(lines):
        stripped = line.lstrip()
        current_indent = len(line) - len(stripped)

        # Check if we're entering a model definition
        if stripped.startswith('- name:'):
            name_value = stripped.replace('- name:', '').strip()
            if name_value == model_name:
                in_target_model = True
                model_indent = current_indent
                name_line = i + 1  # 1-indexed for GitHub
                name_content = line
                continue  # Don't check this line again
            else:
                in_target_model = False

        # If in target model, look for description
        if in_target_model:
            if stripped.startswith('description:'):
                description_line = i + 1
                description_content = line
                desc_indent = current_indent
                # Check if multiline (ends with > or | or doesn't close quote on same line)
                is_multiline = (
                    stripped.endswith('>') or
                    stripped.endswith('|') or
                    (stripped.count('"') == 1) or
                    (stripped.count("'") == 1)
                )
                if is_multiline:
                    # Find the last line of the multiline description
                    for j in range(i + 1, len(lines)):
                        next_line = lines[j]
                        next_stripped = next_line.lstrip()
                        next_indent = len(next_line) - len(next_stripped)
                        # Description ends when we return to same or lesser indent with content
                        if next_stripped and next_indent <= desc_indent:
                            # Previous line was the last of description
                            description_line = j  # j is 0-indexed, but we want line before
                            description_content = lines[j - 1]
                            break
            # If we hit columns or another top-level key, stop
            elif stripped.startswith('columns:') or stripped.startswith('config:'):
                break
            # If we hit another model (same indent with - name:), stop
            elif current_indent == model_indent and stripped.startswith('- name:'):
                break

    # Return line after description, or after name if no description
    if description_line:
        return description_line, ' ' * (model_indent + 2), description_content
    return name_line, ' ' * (model_indent + 2), name_content


def generate_suggestion(author_name: str, indent: str, original_line: str) -> str:
    """Generate the YAML suggestion for owner metadata.

    Includes original line since GitHub suggestions replace rather than insert.
    """
    return f"""{original_line}
{indent}config:
{indent}  meta:
{indent}    owner:
{indent}      name: {author_name}"""


def main() -> int:
    parser = argparse.ArgumentParser(description='Check for missing model ownership')
    parser.add_argument('--author-name', required=True, help='PR author name')
    parser.add_argument('--base-branch', required=True, help='Base branch to compare against')
    parser.add_argument('--output', required=True, help='Output JSON file for suggestions')
    args = parser.parse_args()

    new_files = get_new_sql_files(args.base_branch)
    suggestions = []

    for sql_path in new_files:
        model_name = sql_path.stem
        yaml_path = find_yaml_for_model(sql_path)

        if not yaml_path:
            print(f"Warning: No YAML file found for {model_name}")
            continue

        if model_has_owner(yaml_path, model_name):
            print(f"Skipping {model_name}: already has owner")
            continue

        line, indent, original_line = find_insertion_line(yaml_path, model_name)
        if not line or not original_line:
            print(f"Warning: Could not find insertion point for {model_name}")
            continue

        suggestion_text = generate_suggestion(args.author_name, indent, original_line)

        suggestions.append({
            'file': str(yaml_path).replace('\\', '/'),
            'line': line,
            'model': model_name,
            'suggestion': suggestion_text
        })

        print(f"Will suggest owner for {model_name} at {yaml_path}:{line}")

    # Write suggestions to JSON
    with open(args.output, 'w') as f:
        json.dump(suggestions, f, indent=2)

    print(f"\nGenerated {len(suggestions)} suggestions")
    return 0


if __name__ == '__main__':
    sys.exit(main())
