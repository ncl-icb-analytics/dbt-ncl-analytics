"""
Add dbt_utils.at_least_one test to all staging models.

This script:
1. Finds all staging .sql files
2. Checks if they have corresponding .yml files
3. Adds dbt_utils.at_least_one test if not already present
4. Reports any staging models without .yml files
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Tuple


def find_staging_models(project_root: Path) -> List[Tuple[Path, Path]]:
    """
    Find all staging model SQL files and their corresponding YAML files.

    Returns:
        List of tuples (sql_path, yml_path or None)
    """
    staging_models = []
    models_dir = project_root / "models"

    # Find all SQL files in staging directories
    for sql_file in models_dir.rglob("staging/*.sql"):
        # Get corresponding yml file
        yml_file = sql_file.with_suffix('.yml')

        staging_models.append((sql_file, yml_file if yml_file.exists() else None))

    return staging_models


def has_at_least_one_test(model_config: Dict) -> bool:
    """Check if model already has at_least_one test."""
    tests = model_config.get('tests', [])

    # Check model-level tests
    for test in tests:
        if isinstance(test, str) and 'at_least_one' in test:
            return True
        if isinstance(test, dict) and 'dbt_utils.at_least_one' in test:
            return True

    return False


def add_at_least_one_test(yml_path: Path) -> bool:
    """
    Add dbt_utils.at_least_one test to a model YAML if not present.

    Returns:
        True if test was added, False if already present or error
    """
    try:
        with open(yml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}

        # Ensure models key exists
        if 'models' not in data:
            data['models'] = []

        # Process each model in the YAML
        modified = False
        for model in data['models']:
            if not has_at_least_one_test(model):
                # Add tests key if it doesn't exist
                if 'tests' not in model:
                    model['tests'] = []

                # Add at_least_one test
                model['tests'].append('dbt_utils.at_least_one')
                modified = True

        # Write back if modified
        if modified:
            with open(yml_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            return True

        return False

    except Exception as e:
        print(f"  [!] Error processing {yml_path.name}: {e}")
        return False


def main():
    """Main execution."""
    project_root = Path(__file__).parent.parent.parent

    print("Finding staging models...\n")
    staging_models = find_staging_models(project_root)

    print(f"Found {len(staging_models)} staging model(s)\n")

    # Track statistics
    missing_yml = []
    already_has_test = []
    test_added = []
    errors = []

    for sql_path, yml_path in staging_models:
        model_name = sql_path.stem
        relative_path = sql_path.relative_to(project_root)

        if yml_path is None:
            print(f"[X] {model_name}: No YAML file found ({relative_path})")
            missing_yml.append(relative_path)
        else:
            # Check if test needs to be added
            try:
                with open(yml_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f) or {}

                models = data.get('models', [])
                if models and has_at_least_one_test(models[0]):
                    print(f"[OK] {model_name}: Already has at_least_one test")
                    already_has_test.append(model_name)
                else:
                    if add_at_least_one_test(yml_path):
                        print(f"[+] {model_name}: Added at_least_one test")
                        test_added.append(model_name)
                    else:
                        print(f"[!] {model_name}: Could not add test")
                        errors.append(model_name)

            except Exception as e:
                print(f"[X] {model_name}: Error - {e}")
                errors.append(model_name)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total staging models: {len(staging_models)}")
    print(f"[+] Tests added: {len(test_added)}")
    print(f"[OK] Already had test: {len(already_has_test)}")
    print(f"[X] Missing YAML files: {len(missing_yml)}")
    print(f"[!] Errors: {len(errors)}")

    if missing_yml:
        print("\nModels missing YAML files:")
        for path in missing_yml:
            print(f"   - {path}")

    if errors:
        print("\nModels with errors:")
        for name in errors:
            print(f"   - {name}")


if __name__ == "__main__":
    main()
