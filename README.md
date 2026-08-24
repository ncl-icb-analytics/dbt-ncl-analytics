# WNL ICB Analytics dbt Project

[![Last Commit](https://img.shields.io/github/last-commit/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/commits/main)
[![Commit Activity](https://img.shields.io/github/commit-activity/m/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulse)
[![Open PRs](https://img.shields.io/github/issues-pr/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls)
[![Merged PRs](https://badgen.net/github/merged-prs/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls?q=is%3Amerged)
[![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/EddieDavison92/fe9920551839b7a85d0f47dfd527e62b/raw/coverage.json)](https://github.com/wnl-icb-analytics/dbt-analytics/actions/workflows/test-coverage.yml)
[![License](https://img.shields.io/badge/license-OGL%20v3%20|%20MIT-blue)](LICENSE)

dbt project for WNL ICB Analytics healthcare data transformations on Snowflake.

New to the project? Use the
[dbt onboarding courses and handbook](https://dbt-onboarding.vercel.app/).
Before changing a model, read the concise
[project conventions](PROJECT_CONVENTIONS.md). Setup and pull-request steps
are in [CONTRIBUTING.md](CONTRIBUTING.md).

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup including commit signing.

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
| `.\build_changed.ps1` | Build only changed models (auto-detects from git diff) |

**Flags for `build_changed`:**
- `-u` upstream dependencies
- `-d` downstream dependents
- `-r` run only (no tests)
- `-t` test only

## Common Commands

The VS Code workspace runs `.\start_dbt.ps1` automatically when you open a terminal.

| Command | Description |
|---------|-------------|
| `dbt compile -s model_name` | Compile a model without running it |
| `dbt show -s model_name --limit 20` | Preview model rows |
| `dbt build -s model_name` | Build a model and run its tests |
| `dbt build -s +model_name` | Build a model with upstream dependencies |
| `dbt build -s model_name+` | Build a model and its downstream consumers |
| `dbt ls -s model_name+` | List downstream impact before a change |
| `dbt run -s tag:qof` | Run models by tag |
| `dbt docs generate && dbt docs serve` | Generate and view documentation |

## Project Structure

```text
models/
├── sources/       # Generated and manual source declarations
├── raw/           # Generated 1:1 views of source data
├── staging/       # Required clean interface to raw data
├── reference/     # Shared derived reference datasets
├── modelling/     # Reusable transformations and domain rules
│   ├── acute/
│   ├── community/
│   ├── olids/
│   └── population/
├── reporting/     # Business-ready marts for direct analysis
├── semantic/      # Snowflake semantic views
├── published/     # Governed datasets for named products
└── partner/       # Governed partner datasets and access rules
```

Raw has one route: `DATA_LAKE → Raw → Staging`. Only staging models may
reference `raw_` models. After staging, models reuse the contract that fits;
modelling and reporting may reference each other where the DAG remains acyclic.
Published, partner and semantic models serve downstream consumers.

## Working conventions

- State the subject, population, reference time and grain before writing SQL.
- Search names, YAML and lineage before creating another definition.
- Reuse or extend the canonical staging model for a source object; do not add a
  pipeline- or consumer-named duplicate.
- Keep staging joins exceptional and universal: cleaning, standardisation or
  enrichment that every consumer of the source should inherit.
- Choose model areas by responsibility and consumers. Do not bypass staging to
  read raw data.
- Name models for their subject and grain, not for processing steps such as
  consolidation or enrichment.
- Prefer readable, straightforward SQL. Logic must not masquerade as data: keep
  executable rules in SQL and use seeds for declarative facts or parameters.
- Use unquoted `snake_case` output columns from staging onward. Published models
  may instead use quoted friendly names for consumers; this is optional.
- Do not add columns known to be null for every row unless a documented
  published-output contract requires a fixed-schema placeholder.
- This repository is public. Never commit patient- or person-level data,
  identifying values, row-level extracts or screenshots containing real data.
  High-level aggregate counts, rates and validation totals are not person-level
  data when they cannot identify an individual.
- Treat SQL, descriptions, ownership and tests as one model change.
- Test the key or key combination that enforces grain. Check that joins do not
  multiply rows unless the model documents the new grain.
- Split confused responsibilities, not individual transformations or CTEs.
- Use `ref()` in hand-written models; only generated raw models use `source()`.
- Keep programme, geography, legacy, audience and product rules at the scope
  that owns them. Make narrower scope visible in the model's folder/schema,
  name and description.

See [Project conventions](PROJECT_CONVENTIONS.md) for the review checklist
and [Working with Sources](docs/working-with-sources.md) for source generation.

## Learning dbt

The [dbt onboarding site](https://dbt-onboarding.vercel.app/) contains interactive
courses on git, dbt and a first pull request, plus the project handbook. It is the
canonical learning guide for this project. You only need the `ANALYST` role to
follow along.

For general dbt learning: [dbt Fundamentals](https://learn.getdbt.com/courses/dbt-fundamentals-vs-code) | [dbt Learn catalog](https://learn.getdbt.com/catalog) | [dbt Documentation](https://docs.getdbt.com/) | [dbt Community Slack](https://www.getdbt.com/community/)

## Project reference

| Resource | Description |
|----------|-------------|
| [Project conventions](PROJECT_CONVENTIONS.md) | Model contracts, layer boundaries, naming, tests and review checks |
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
| MODELLING | Named, tested transformations and shared domain rules |
| REPORTING | Analytics-ready datasets with business metrics |
| REPORTING.SEMANTIC | Snowflake semantic views over established models |
| PUBLISHED_REPORTING__SECONDARY_USE | Population health and operational analytics |
| PUBLISHED_REPORTING__DIRECT_CARE | Individual patient care (consent-based access) |
| PUBLISHED_REPORTING__PARTNER | Governed partner datasets |

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
| `models/semantic/` | `DEV__REPORTING.SEMANTIC` | `REPORTING.SEMANTIC` |
| `models/published/direct_care/olids/` | `DEV__PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_*` | `PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_*` |
| `models/partner/published_data/` | `DEV__PUBLISHED_REPORTING__PARTNER.PUBLISHED_DATA` | `PUBLISHED_REPORTING__PARTNER.PUBLISHED_DATA` |

**How it works:**
- **Database**: Set by `+database` in `dbt_project.yml`, prefixed with `DEV__` in dev
- **Schema**: Either explicit (`+schema`), the source-system folder for staging models, or auto-derived from folder path for the `olids` domain

The naming logic is in `macros/overrides/generate_database_name.sql` and `generate_schema_name.sql`.

### Technology Stack

- **dbt Fusion engine** - Rust-based dbt runtime used locally and in GitHub Actions
- **Snowflake** - Cloud data warehouse
- **Python 3.11** - Helper scripts in `scripts/` only (not dbt)

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.
