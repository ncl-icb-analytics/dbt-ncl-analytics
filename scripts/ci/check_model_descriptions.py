#!/usr/bin/env python3
"""Check that all models have descriptions.

Checks for descriptions in:
1. YAML files (schema.yml, model_name.yml, etc.)
2. SQL config blocks: {{ config(description='...') }}

Only checks files passed as arguments (typically changed files in a PR).
"""

import re
import sys
from pathlib import Path

import yaml

# Pattern to find description in SQL config block (single or double quotes)
CONFIG_DESC_PATTERN = r"{{\s*config\s*\([^)]*description\s*=\s*['\"](.+?)['\"]"


def find_yaml_files(model_path: Path) -> list[Path]:
    """Find YAML files that might contain the model definition."""
    model_dir = model_path.parent
    yaml_files = []

    yaml_files.extend(model_dir.glob('*.yml'))
    yaml_files.extend(model_dir.glob('*.yaml'))

    if model_dir.parent.exists():
        yaml_files.extend(model_dir.parent.glob('*.yml'))
        yaml_files.extend(model_dir.parent.glob('*.yaml'))

    return yaml_files


def has_description_in_yaml(model_name: str, yaml_files: list[Path]) -> bool:
    """Check if model has a description in any of the YAML files."""
    for yaml_file in yaml_files:
        try:
            content = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
            if not content or 'models' not in content:
                continue

            for model in content.get('models', []):
                if model.get('name') == model_name:
                    desc = model.get('description', '')
                    if desc and desc.strip():
                        return True
        except (yaml.YAMLError, Exception):
            continue

    return False


def has_description_in_sql(sql_path: Path) -> bool:
    """Check if model has a description in its SQL config block."""
    try:
        content = sql_path.read_text(encoding='utf-8', errors='ignore')
        match = re.search(CONFIG_DESC_PATTERN, content, re.DOTALL | re.IGNORECASE)
        if match and match.group(1).strip():
            return True
    except Exception:
        pass
    return False


def model_has_description(model_name: str, sql_path: Path, yaml_files: list[Path]) -> bool:
    """Check if model has a description in YAML or SQL config."""
    return has_description_in_yaml(model_name, yaml_files) or has_description_in_sql(sql_path)


def main() -> int:
    if len(sys.argv) < 2:
        print("PASSED: No files to check.")
        return 0

    files = [Path(f) for f in sys.argv[1:] if f.endswith('.sql')]
    missing_descriptions: list[str] = []

    for filepath in files:
        if not filepath.exists():
            continue
        if 'dbt_packages' in str(filepath):
            continue

        path_str = str(filepath).replace('\\', '/')
        if not path_str.startswith('models/'):
            continue

        model_name = filepath.stem
        yaml_files = find_yaml_files(filepath)

        if not model_has_description(model_name, filepath, yaml_files):
            missing_descriptions.append(str(filepath))

    if missing_descriptions:
        print("FAILED: Models missing descriptions:\n")
        for filepath in sorted(missing_descriptions):
            print(f"  - {filepath}")
        print("\nAdd a description for each model in a corresponding .yml file.")
        print("See: https://docs.getdbt.com/reference/resource-properties/description")
        return 1

    print("PASSED: All models have descriptions.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
