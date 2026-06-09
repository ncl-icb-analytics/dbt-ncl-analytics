#!/usr/bin/env python3
"""Check for (or fix) the deprecated `tests:` YAML key in schema files.

dbt 1.8+ renamed the schema property `tests:` -> `data_tests:`. This script
flags files still using the legacy key and can rewrite them in place.

Usage:
    check_tests_key.py [files...]          # check mode: FAILED + exit 1 if found
    check_tests_key.py --fix [files...]    # rewrite tests: -> data_tests:

With no file arguments, scans models/, seeds/ and snapshots/.

Fix mode verifies each rewrite structurally: the file is re-parsed and must
equal the original document with only `tests` keys renamed. Files that fail
verification (or don't parse) are left untouched and reported.
"""

import re
import sys
from pathlib import Path

# Key at start of line (any indent), colon followed by whitespace or EOL.
# Matches `tests:`, `tests: []`, `tests: &anchor`, `tests: *alias`.
KEY_RE = re.compile(r'^([ \t]*)tests:(?=\s|$)', re.MULTILINE)

DEFAULT_DIRS = ('models', 'seeds', 'snapshots')


def rename_keys(obj):
    """Return obj with every `tests` mapping key renamed to `data_tests`."""
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if key == 'tests' and 'data_tests' in obj:
                raise ValueError("mapping has both 'tests' and 'data_tests'")
            out['data_tests' if key == 'tests' else key] = rename_keys(value)
        return out
    if isinstance(obj, list):
        return [rename_keys(item) for item in obj]
    return obj


def fix_file(path: Path) -> tuple[int, str | None]:
    """Rewrite legacy keys in path. Returns (replacements, error)."""
    import yaml  # only --fix needs pyyaml; check mode stays stdlib-only

    with open(path, encoding='utf-8', newline='') as f:
        original = f.read()

    rewritten, count = KEY_RE.subn(r'\1data_tests:', original)
    if count == 0:
        return 0, None

    try:
        expected = rename_keys(yaml.safe_load(original))
    except (yaml.YAMLError, ValueError) as e:
        return 0, f"cannot verify ({e}); fix manually"

    if yaml.safe_load(rewritten) != expected:
        return 0, "rewrite changed more than the key; fix manually"

    with open(path, 'w', encoding='utf-8', newline='') as f:
        f.write(rewritten)
    return count, None


def collect_files(args: list[str]) -> list[Path]:
    if args:
        return [Path(a) for a in args
                if a.endswith(('.yml', '.yaml')) and Path(a).exists()]
    files = []
    for d in DEFAULT_DIRS:
        files.extend(Path(d).rglob('*.yml'))
        files.extend(Path(d).rglob('*.yaml'))
    return files


def main() -> int:
    argv = sys.argv[1:]
    fix = '--fix' in argv
    files = collect_files([a for a in argv if a != '--fix'])

    flagged = {f: len(KEY_RE.findall(f.read_text(encoding='utf-8')))
               for f in files}
    flagged = {f: n for f, n in flagged.items() if n}

    if not flagged:
        print("PASSED: No legacy `tests:` keys found.")
        return 0

    if not fix:
        print(f"FAILED: Legacy `tests:` key in {len(flagged)} file(s) "
              "(dbt 1.8+ uses `data_tests:`):\n")
        for f in sorted(flagged):
            print(f"  - {f} ({flagged[f]})")
        print("\nFix with: python scripts/utils/check_tests_key.py --fix <files>")
        return 1

    total = 0
    errors = []
    for f in sorted(flagged):
        count, error = fix_file(f)
        if error:
            errors.append(f"  - {f}: {error}")
        else:
            total += count
    print(f"Renamed {total} keys in {len(flagged) - len(errors)} file(s).")
    if errors:
        print(f"\nFAILED: {len(errors)} file(s) not fixed:")
        print('\n'.join(errors))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
