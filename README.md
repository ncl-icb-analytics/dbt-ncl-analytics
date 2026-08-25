# WNL ICB Analytics dbt Project

[![Last Commit](https://img.shields.io/github/last-commit/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/commits/main)
[![Commit Activity](https://img.shields.io/github/commit-activity/m/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulse)
[![Open PRs](https://img.shields.io/github/issues-pr/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls)
[![Merged PRs](https://badgen.net/github/merged-prs/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls?q=is%3Amerged)
[![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/EddieDavison92/fe9920551839b7a85d0f47dfd527e62b/raw/coverage.json)](https://github.com/wnl-icb-analytics/dbt-analytics/actions/workflows/test-coverage.yml)
[![License](https://img.shields.io/badge/license-OGL%20v3%20|%20MIT-blue)](LICENSE)

dbt project for WNL ICB Analytics healthcare data transformations.

## Quick Start

```powershell
# Clone
git clone https://github.com/wnl-icb-analytics/dbt-analytics && cd dbt-analytics

# Configure credentials
Copy-Item env.example .env    # Edit with your Snowflake credentials

# Bootstrap: installs the dbt Fusion engine, configures git hooks, loads .env
.\start_dbt.ps1
dbt deps
dbt debug
```

dbt runs on the Fusion engine (installed by `start_dbt.ps1`), not a Python
package. `uv` is only needed for the Python helper scripts in `scripts/`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed setup including commit signing.

## Codespaces

Cloud dev with no local install: add your Snowflake secrets once (a PAT is the
recommended auth), create a codespace, and Fusion + packages are set up
automatically. See **[Developing in GitHub Codespaces](docs/codespaces.md)**.

## What This Project Does

Transforms healthcare data into analytical datasets across two domains:

- **Commissioning** - Secondary care activity, waiting lists, community and mental health services
- **OLIDS** - QOF disease registers, clinical programmes, population health metrics

Data sources: OLIDS (GP data), SUS (secondary care), Waiting Lists, CSDS/MHSDS, EPD (prescribing), eRS (referrals).

## Helper Scripts

| Script | Description |
|--------|-------------|
| `.\start_dbt.ps1` | Installs/updates dbt Fusion, configures hooks, loads `.env` (auto-runs on terminal open) |
| `.\build_changed` | Build only changed models (auto-detects from git diff) |

**Flags for `build_changed`:**
- `-u` upstream dependencies
- `-d` downstream dependents
- `-r` run only (no tests)
- `-t` test only

## Common Commands

The VS Code workspace runs `.\start_dbt.ps1` automatically when you open a terminal.

| Command | Description |
|---------|-------------|
| `dbt build` | Build all models and run tests |
| `dbt run -s model_name` | Run a specific model |
| `dbt run -s +model_name` | Run model with upstream dependencies |
| `dbt run -s tag:qof` | Run models by tag |
| `dbt test -s model_name` | Test a specific model |
| `dbt docs generate && dbt docs serve` | Generate and view documentation |

## Project Structure

```
models/
├── raw/           # 1:1 views of source data
├── staging/       # Cleaned and standardised
├── reference/     # Derived reference datasets
├── modelling/     # Business logic and transformations
│   ├── acute/
│   ├── community/
│   ├── olids/
│   └── population/
├── reporting/     # Analytics-ready datasets
└── published/     # External reports and dashboards
```

Data flows: `DATA_LAKE → Raw → Staging → Modelling → Reporting → Published`

## Learning dbt

New to dbt or this project? Start at **[dbt-onboarding.vercel.app](https://dbt-onboarding.vercel.app/)** —
our interactive courses (git, what dbt is, and a hands-on first PR) plus a handbook,
all written around our environment and conventions. It is the canonical source for
learning dbt here. You only need the `ANALYST` role to follow along.

For general dbt learning: [dbt Fundamentals](https://learn.getdbt.com/courses/dbt-fundamentals-vs-code) | [dbt Learn catalog](https://learn.getdbt.com/catalog) | [dbt Documentation](https://docs.getdbt.com/) | [dbt Community Slack](https://www.getdbt.com/community/)

## Project reference

| Resource | Description |
|----------|-------------|
| [CONTRIBUTING.md](CONTRIBUTING.md) | Quick-start: setup, commit signing, workflow |
| [GitHub Codespaces](docs/codespaces.md) | Cloud dev: add secrets, create a codespace, how auth works |
| [GitHub Actions](docs/github-actions.md) | CI/CD pipelines, deployment, project automations |
| [Working with Sources](docs/working-with-sources.md) | Adding sources, regenerating raw models, and handling drift |
| [SLAM Data](docs/slam-data-guide.md) | Source-to-staging methodology for the SLAM contract feeds |
| [SUS Models](docs/sus-models.md) | Secondary care (SUS) model structure |

Older learning guides now live in [docs/archive/](docs/archive/), superseded by the onboarding site.

## Architecture

### Database Layers

| Layer | Purpose |
|-------|---------|
| DATA_LAKE | Raw data with 1:1 views of external sources |
| STAGING | Raw passthrough views (DBT_RAW) and cleaned source data (schema per source) |
| MODELLING | Transformations: filter, reshape, categorise, link |
| REPORTING | Analytics-ready datasets with business metrics |
| PUBLISHED_REPORTING__SECONDARY_USE | Population health and operational analytics |
| PUBLISHED_REPORTING__DIRECT_CARE | Individual patient care (consent-based access) |

Development uses `DEV__` prefixed databases (e.g., `DEV__MODELLING`).

### Where Models Land in Snowflake

| Model Folder | Dev | Prod |
|--------------|-----|------|
| `models/raw/` | `DEV__STAGING.DBT_RAW` | `STAGING.DBT_RAW` |
| `models/staging/commissioning/csds/` | `DEV__STAGING.CSDS` | `STAGING.CSDS` |
| `models/staging/olids/` | `DEV__STAGING.OLIDS` | `STAGING.OLIDS` |
| `models/reference/organisation/` | `DEV__REFERENCE.ORGANISATION` | `REFERENCE.ORGANISATION` |
| `models/modelling/acute/` | `DEV__MODELLING.ACUTE` | `MODELLING.ACUTE` |
| `models/modelling/olids/diagnoses/` | `DEV__MODELLING.OLIDS_DIAGNOSES` | `MODELLING.OLIDS_DIAGNOSES` |
| `models/reporting/population/` | `DEV__REPORTING.POPULATION` | `REPORTING.POPULATION` |
| `models/reporting/olids/indicators/` | `DEV__REPORTING.OLIDS_INDICATORS` | `REPORTING.OLIDS_INDICATORS` |
| `models/modelling/commissioning/legacy_nwl/` | `DEV__MODELLING.LEGACY_NWL` | `MODELLING.LEGACY_NWL` |
| `models/reporting/commissioning/legacy_nwl/` | `DEV__REPORTING.LEGACY_NWL` | `REPORTING.LEGACY_NWL` |
| `models/published/direct_care/olids/` | `DEV__PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_*` | `PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_*` |

**How it works:**
- **Database**: Set by `+database` in `dbt_project.yml`, prefixed with `DEV__` in dev
- **Schema**: Either explicit (`+schema`), the source-system folder for staging models, or auto-derived from folder path for the `olids` domain

`commissioning/legacy_nwl` holds transitional models ported from the WSIC/ERNI estate.
They override the domain schema so they do not sit alongside the long-term models, and
are removed once the equivalent estate-wide chain covers them. Subfolders are free-form —
everything under `legacy_nwl/` lands in the same schema. Select the set with
`--select tag:legacy_nwl`. Raw and staging models are shared and stay in the normal folders.

The naming logic is in `macros/overrides/generate_database_name.sql` and `generate_schema_name.sql`.

### Technology Stack

- **dbt Fusion engine** - Rust-based dbt runtime (local dev + Snowflake native execution)
- **Snowflake** - Cloud data warehouse
- **Python 3.11** - Helper scripts in `scripts/` only (not dbt)

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.
