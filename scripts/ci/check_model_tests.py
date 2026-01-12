#!/usr/bin/env python3
"""Check that all models have at least one test defined.

Only checks files passed as arguments (typically changed files in a PR).
Tests can be defined at the model level or column level.
"""

import sys
from pathlib import Path

import yaml


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


def model_has_tests(model_name: str, yaml_files: list[Path]) -> bool:
    """Check if model has at least one test in any of the YAML files."""
    for yaml_file in yaml_files:
        try:
            content = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
            if not content or 'models' not in content:
                continue

            for model in content.get('models', []):
                if model.get('name') == model_name:
                    # Check model-level tests
                    if model.get('tests'):
                        return True
                    # Check data_tests (dbt 1.8+ format)
                    if model.get('data_tests'):
                        return True

                    # Check column-level tests
                    for column in model.get('columns', []):
                        if column.get('tests'):
                            return True
                        if column.get('data_tests'):
                            return True

        except (yaml.YAMLError, Exception):
            continue

    return False


def main() -> int:
    if len(sys.argv) < 2:
        print("PASSED: No files to check.")
        return 0

    files = [Path(f) for f in sys.argv[1:] if f.endswith('.sql')]
    missing_tests: list[str] = []

    for filepath in files:
        if not filepath.exists():
            continue
        if 'dbt_packages' in str(filepath):
            continue

        path_str = str(filepath).replace('\\', '/')
        if not path_str.startswith('models/'):
            continue

        # Skip raw/ layer - tests not required
        if '/raw/' in path_str:
            continue

        model_name = filepath.stem
        yaml_files = find_yaml_files(filepath)

        if not model_has_tests(model_name, yaml_files):
            missing_tests.append(str(filepath))

    if missing_tests:
        print("FAILED: Models missing tests:\n")
        for filepath in sorted(missing_tests):
            print(f"  - {filepath}")
        print("\nAdd at least one test for each model in a corresponding .yml file.")
        print("See: https://docs.getdbt.com/docs/build/data-tests")
        return 1

    print("PASSED: All models have tests.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
