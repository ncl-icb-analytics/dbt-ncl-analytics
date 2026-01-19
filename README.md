# NCL Analytics dbt Project

[![Last Commit](https://img.shields.io/github/last-commit/ncl-icb-analytics/dbt-ncl-analytics)](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/commits/main)
[![Commit Activity](https://img.shields.io/github/commit-activity/m/ncl-icb-analytics/dbt-ncl-analytics)](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/pulse)
[![Open PRs](https://img.shields.io/github/issues-pr/ncl-icb-analytics/dbt-ncl-analytics)](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/pulls)
[![Merged PRs](https://badgen.net/github/merged-prs/ncl-icb-analytics/dbt-ncl-analytics)](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/pulls?q=is%3Amerged)
[![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/EddieDavison92/fe9920551839b7a85d0f47dfd527e62b/raw/coverage.json)](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/actions/workflows/test-coverage.yml)
[![License](https://img.shields.io/badge/license-OGL%20v3%20|%20MIT-blue)](LICENSE)

dbt project for NCL ICB Analytics healthcare data transformations.

## Quick Start

```bash
# Install uv if you don't have it
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Clone and setup
git clone https://github.com/ncl-icb-analytics/dbt-ncl-analytics && cd dbt-ncl-analytics
uv sync
cp env.example .env    # Edit with your Snowflake credentials
.\start_dbt.ps1 && dbt deps && dbt debug
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed setup including commit signing.

## What This Project Does

Transforms healthcare data into analytical datasets across two domains:

- **Commissioning** - Secondary care activity, waiting lists, community and mental health services
- **OLIDS** - QOF disease registers, clinical programmes, population health metrics

Data sources: OLIDS (GP data), SUS (secondary care), Waiting Lists, CSDS/MHSDS, EPD (prescribing), eRS (referrals).

## Helper Scripts

| Script | Description |
|--------|-------------|
| `.\start_dbt.ps1` | **Run first** - Loads `.env` credentials into your session |
| `.\build_changed` | Build only changed models (auto-detects from git diff) |

**Flags for `build_changed`:**
- `-u` upstream dependencies
- `-d` downstream dependents
- `-r` run only (no tests)
- `-t` test only

## Common Commands

Always run `.\start_dbt.ps1` first in each terminal session.

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
├── modelling/     # Business logic and transformations
│   ├── commissioning/
│   ├── olids/
│   └── shared/
├── reporting/     # Analytics-ready datasets
└── published/     # External reports and dashboards
```

Data flows: `DATA_LAKE → Raw → Staging → Modelling → Reporting → Published`

## Documentation

| Resource | Description |
|----------|-------------|
| [CONTRIBUTING.md](CONTRIBUTING.md) | Setup, commit signing, workflow |
| [Development Guide](docs/development-guide.md) | Daily workflows, advanced patterns |
| [Working with Sources](docs/working-with-sources.md) | Source generation workflow |
| [CHANGELOG.md](CHANGELOG.md) | Release history |

## Learning dbt

New to dbt? Start with [dbt Fundamentals](https://learn.getdbt.com/courses/dbt-fundamentals-vs-code) or browse the [dbt Learn catalog](https://learn.getdbt.com/catalog).

See also: [dbt Documentation](https://docs.getdbt.com/) | [dbt Community Slack](https://www.getdbt.com/community/)

## Architecture

### Database Layers

| Layer | Purpose |
|-------|---------|
| DATA_LAKE | Raw data with 1:1 views of external sources |
| MODELLING | Transformations: filter, reshape, categorise, link |
| REPORTING | Analytics-ready datasets with business metrics |
| PUBLISHED_REPORTING__SECONDARY_USE | Population health and operational analytics |
| PUBLISHED_REPORTING__DIRECT_CARE | Individual patient care (consent-based access) |

Development uses `DEV__` prefixed databases (e.g., `DEV__MODELLING`).

### Technology Stack

- **dbt-core 1.9.4** - Do not upgrade to 1.10+
- **Snowflake** - Cloud data warehouse
- **Python 3.8+** - Scripting and automation

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.
