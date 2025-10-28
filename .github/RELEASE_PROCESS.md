# Release Process

This document explains how automated releases work in this dbt project.

## Overview

Releases are automatically created when pull requests are merged to `main`. The system uses [release-please](https://github.com/googleapis/release-please) to generate releases based on [Conventional Commits](https://www.conventionalcommits.org/).

## How It Works

### 1. Conventional Commits

All commits must follow the Conventional Commits standard:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types that trigger releases:**
- `feat:` - New features/models (increments MINOR version)
- `fix:` - Bug fixes (increments PATCH version)
- `refactor:` - Code refactoring (increments PATCH version)
- `docs:` - Documentation updates (increments PATCH version)
- `test:` - Test additions/changes (increments PATCH version)

**Type that triggers major version:**
- Any commit with `BREAKING CHANGE:` in the footer (increments MAJOR version)

**Examples:**
```
feat(smi): add population base model
fix(bp-control): correct monitoring interval thresholds
refactor(olids): reorganise published models into subdomain folders
docs(valproate): update medication matching documentation
```

### 2. Release Workflow

When a PR is merged to `main`:

1. **Release Please creates a Release PR**
   - Automatically updates version in `dbt_project.yml`
   - Updates `CHANGELOG.md` with grouped changes
   - Analyses conventional commits to determine version bump

2. **When Release PR is merged:**
   - Creates a GitHub release with tag (e.g., `v1.2.3`)
   - Generates release notes grouped by type
   - Runs dbt model analysis script
   - Appends comprehensive model change summary

### 3. Release Notes Structure

Each release includes:

#### Conventional Commit Summary
```markdown
## What's Changed

### Features
- feat(smi): make view for SMI pop base (#142)
- feat(smi): create smi population base (#139)

### Fixes
- fix(bp-control): update monitoring interval thresholds (#143)
```

#### dbt Models Changed
```markdown
## dbt Models Changed

✨ Added: models/modelling/olids/smi/smi_population_base.sql → OLIDS_SMI
📝 Modified: models/modelling/olids/bp_control/bp_monitoring.sql → OLIDS_BP_CONTROL
```

#### Impact Summary
```markdown
## Impact Summary

| Layer | Database | Added | Modified | Deleted |
|-------|----------|-------|----------|---------|
| Staging | MODELLING | 1 | 0 | 0 |
| Modelling | MODELLING | 2 | 3 | 0 |
| Published | PUBLISHED_REPORTING | 0 | 1 | 0 |

**Schemas affected**: DBT_STAGING, OLIDS_SMI, OLIDS_BP_CONTROL
```

## Versioning Strategy

We use **Semantic Versioning** (semver): `vMAJOR.MINOR.PATCH`

- **MAJOR** (v2.0.0): Breaking changes, major refactors, schema reorganisation
  - Requires `BREAKING CHANGE:` in commit footer
  - Should be rare and intentional

- **MINOR** (v1.1.0): New features, new models added
  - Triggered by `feat:` commits
  - Adding new SQL models
  - New functionality

- **PATCH** (v1.0.1): Bug fixes, documentation, non-breaking refactors
  - Triggered by `fix:`, `docs:`, `refactor:`, `test:` commits
  - Modifying existing models
  - Documentation improvements

## Working with Releases

### Viewing Releases

```bash
# List all releases
gh release list

# View specific release
gh release view v1.2.3

# View latest release
gh release view --web
```

### Manual Release (if needed)

The Release Please PR can be manually edited before merging if you need to:
- Adjust version number
- Modify changelog entries
- Add additional context

### Finding Changes Between Releases

```bash
# Compare two releases
git diff v1.2.0..v1.3.0 -- models/

# See all models changed since last release
git diff v1.2.0..HEAD --name-only -- models/**/*.sql

# View commits since last release
git log v1.2.0..HEAD --oneline
```

### Linking Deployments to Releases

Use release tags to track what's deployed:

```bash
# Tag current production state
git tag -a prod-2025-10-26 v1.3.0 -m "Deployed to production"

# View what's in production
git describe --tags prod-2025-10-26
```

## Best Practices

1. **Write descriptive commit messages**: They become your release notes
2. **Use scopes**: Help categorise changes (e.g., `feat(smi):`, `fix(bp-control):`)
3. **Mention model names**: Even though git diff captures all changes, mentioning models in commits helps reviewers
4. **One feature per PR**: Makes releases cleaner and easier to understand
5. **Review Release PR**: Check the generated changelog before merging

## Troubleshooting

### Release Please PR not created

Check that:
- Commits follow conventional commit format
- PR was merged to `main` branch
- GitHub Actions workflow has permissions to create PRs

### Wrong version bump

Edit the Release Please PR:
- Manually adjust version in `dbt_project.yml`
- Update `CHANGELOG.md` accordingly

### Missing model in release notes

The git diff analysis captures all `.sql` changes automatically. If models are missing:
- Check the workflow logs
- Verify files are in `models/` directory
- Ensure files have `.sql` extension

## Configuration Files

- **Workflow**: `.github/workflows/release-please.yml`
- **Config**: `.github/release-please-config.json`
- **Manifest**: `.github/.release-please-manifest.json`
- **Analysis Script**: `.github/scripts/analyse-dbt-changes.sh`
- **Changelog**: `CHANGELOG.md`
Release-please is now configured and ready!
