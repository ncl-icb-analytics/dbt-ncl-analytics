# GitHub Actions Automations

This project uses GitHub Actions to automate code quality checks, deployment, test coverage reporting, and project management. This document describes each workflow and how they fit together.

## Overview

```
PR opened/updated
  ├── auto-author-assign      Assigns PR author
  ├── dbt-code-quality         Linting and standards checks
  ├── dbt-pr-validation        Builds changed models in Snowflake DEV
  └── model-ownership          Suggests ownership metadata

Push to non-main branch
  └── project-status-in-progress   Marks referenced issues "In Progress"

Merge to main
  ├── dbt-deploy               Deploys changed models to Snowflake PROD
  └── test-coverage            Updates the test coverage badge

Issue labeled "Blocked"
  └── project-status-blocked   Sets project status to "Blocked"

Reviewer requested on PR
  └── project-status-review    Sets project status to "Code Review"
```

## Pull Request Workflows

### Auto Author Assign

**File:** `.github/workflows/auto-author-assign.yml`
**Triggers:** Pull request opened or reopened

Automatically assigns the PR author as an assignee on the pull request. Uses the [`toshimaru/auto-author-assign`](https://github.com/toshimaru/auto-author-assign) action.

### dbt Code Quality

**File:** `.github/workflows/dbt-code-quality.yml`
**Triggers:** Pull request targeting `main`

Runs four parallel code quality checks on changed dbt files:

| Job | What it checks | Script |
|-----|---------------|--------|
| **hardcoded-references** | No hardcoded table/database references in SQL | `scripts/ci/check_hardcoded_refs.sh` |
| **staging-references** | Raw/source references only appear in staging models | `scripts/ci/check_staging_refs.sh` |
| **model-descriptions** | All changed models have descriptions in YAML | `scripts/ci/check_model_descriptions.sh` |
| **model-tests** | All changed models have associated tests | `scripts/ci/check_model_tests.sh` |

Each job only runs if relevant files (`.sql` in `models/`, `macros/`, or `analyses/`) were changed. Results are posted to the GitHub Actions step summary.

### dbt PR Validation

**File:** `.github/workflows/dbt-pr-validation.yml`
**Triggers:** Pull request targeting `main` (on review requested, synchronize, or labeled), or manual dispatch

Builds changed dbt models in the Snowflake **DEV** environment to validate they compile and run before merging. This workflow has specific activation requirements -- it only runs when:

- A reviewer is assigned to the PR, or
- The `snowflake-ci` label is added, or
- A new commit is pushed while one of the above conditions is already met

**How it works:**

1. Detects changed `.sql` files in `models/` and `macros/`
2. For changed YAML files, finds the corresponding SQL models
3. For changed macros, finds all models that reference the macro
4. Queues behind other PR validations using [Turnstyle](https://github.com/softprops/turnstyle) (only one validation runs at a time due to Snowflake project constraints)
5. Connects to Snowflake and runs `EXECUTE DBT PROJECT` against the CI project
6. If more than 100 files changed, runs a full build; otherwise builds only the changed models

**Concurrency:** Cancels in-progress runs when new commits are pushed to the same PR.

### Model Ownership

**File:** `.github/workflows/model-ownership.yml`
**Triggers:** Pull request targeting `main` when `models/**/*.sql` files change

Checks whether changed models have ownership metadata (e.g. `meta.owner`) and posts inline review comments suggesting additions. Uses `scripts/ownership/check_model_ownership.py` to generate suggestions and avoids posting duplicate comments.

## Deployment Workflows

### dbt Deploy

**File:** `.github/workflows/dbt-deploy.yml`
**Triggers:** Push to `main` (when `models/` or `macros/` change), or manual dispatch

Deploys changed dbt models and their downstream dependents to Snowflake **PROD**.

**How it works:**

1. Detects changed files using the same logic as PR validation
2. Writes Snowflake credentials (RSA private key) to a temporary file
3. Connects to Snowflake and executes the dbt project:
   - **0 or 50+ files changed:** Full build
   - **1-49 files changed:** Builds each changed model plus its downstream dependents (`--select model+`)
4. Cleans up credentials (runs even on failure)

**Concurrency:** Queued -- deploys run sequentially, never cancelled.

**Secrets used:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE__USERNAME`, `SNOWFLAKE__PRIVATE_KEY`, `SNOWFLAKE__PASSPHRASE`

### Test Coverage

**File:** `.github/workflows/test-coverage.yml`
**Triggers:** Push to `main` (when `models/` change), or manual dispatch

Scans all dbt models and checks whether each has at least one test defined (model-level or column-level) in its YAML configuration. Updates a [shields.io](https://shields.io/) badge via a GitHub Gist with the coverage percentage.

**Coverage thresholds:**

| Coverage | Badge colour |
|----------|-------------|
| >= 80% | Bright green |
| >= 60% | Green |
| >= 40% | Yellow |
| < 40% | Red |

**Secrets used:** `GIST_TOKEN`

## Project Management Workflows

These workflows keep the GitHub Projects board in sync with development activity.

### Project Status: In Progress

**File:** `.github/workflows/project-status-in-progress.yml`
**Triggers:** Push to any branch except `main`

Scans commit messages and branch names for issue references (e.g. `#123`, `fixes #45`) and for each referenced issue:

- Assigns the pusher to the issue
- Adds the issue to the project board
- Sets status to "In Progress" (only if currently "Todo", "Backlog", or unset)
- Sets the start date to today (if not already set)

### Project Status: Blocked

**File:** `.github/workflows/project-status-blocked.yml`
**Triggers:** Issue labeled with "Blocked"

Adds the issue to the project board and sets its status to "Blocked".

### Project Status: Code Review

**File:** `.github/workflows/project-status-review.yml`
**Triggers:** Review requested on a PR, or PR marked ready for review (with reviewers assigned)

Adds the PR to the project board and sets its status to "Code Review". Skips draft PRs.

## Utility Scripts

### analyse-dbt-changes.sh

**File:** `.github/scripts/analyse-dbt-changes.sh`

A utility script for impact analysis of dbt model changes. Compares changes between git tags and categorises affected files by layer (Raw, Staging, Modelling, Reporting, Published) and target database. Outputs a markdown summary table.

## Secrets Reference

| Secret | Used by | Purpose |
|--------|---------|---------|
| `SNOWFLAKE_ACCOUNT` | deploy, pr-validation | Snowflake account identifier |
| `SNOWFLAKE__USERNAME` | deploy, pr-validation | Snowflake service account username |
| `SNOWFLAKE__PRIVATE_KEY` | deploy, pr-validation | RSA private key for Snowflake authentication |
| `SNOWFLAKE__PASSPHRASE` | deploy, pr-validation | Passphrase for the RSA private key |
| `PROJECT_TOKEN` | project-status-* | GitHub token with project write access |
| `GIST_TOKEN` | test-coverage | GitHub token for updating the coverage badge gist |
