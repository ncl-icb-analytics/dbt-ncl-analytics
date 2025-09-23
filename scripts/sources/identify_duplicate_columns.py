import yaml
import os
import re
from collections import defaultdict

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
STAGING_DIRS = [
    os.path.join(PROJECT_DIR, 'models', 'shared', 'staging'),
    os.path.join(PROJECT_DIR, 'models', 'commissioning', 'staging'),
    os.path.join(PROJECT_DIR, 'models', 'olids', 'staging')
]

def find_duplicate_columns():
    """Find all staging models with _1 suffix columns"""
    duplicates = {}

    for staging_dir in STAGING_DIRS:
        if not os.path.exists(staging_dir):
            continue

        for filename in os.listdir(staging_dir):
            if not filename.endswith('.sql'):
                continue

            filepath = os.path.join(staging_dir, filename)
            with open(filepath, 'r') as f:
                content = f.read()

            # Find lines with _1, _2, etc suffixes
            pattern = r'"([^"]+)" as (\w+)_(\d+)'
            matches = re.findall(pattern, content)

            if matches:
                duplicates[filename] = matches

    return duplicates

def main():
    duplicates = find_duplicate_columns()

    if not duplicates:
        print("No duplicate columns with numeric suffixes found.")
        return

    print("Files with duplicate columns (_1, _2, etc suffixes):")
    print("=" * 70)

    for filename, matches in sorted(duplicates.items()):
        print(f"\n{filename}:")
        for source_col, base_name, suffix in matches:
            print(f"  - Source: {source_col:40} -> {base_name}_{suffix}")

    print("\n" + "=" * 70)
    print(f"Total files with duplicates: {len(duplicates)}")
    print("\nThese duplicates occur when source tables have columns that")
    print("sanitize to the same name (e.g., 'SK_OrganisationID' and 'SK_Organisation_ID')")
    print("\nRecommendation: Check if these are truly different columns in the source,")
    print("or if they're redundant and one should be excluded.")

if __name__ == '__main__':
    main()