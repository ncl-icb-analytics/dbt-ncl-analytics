# WNL ICB Analytics dbt project

[![Last Commit](https://img.shields.io/github/last-commit/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/commits/main)
[![Commit Activity](https://img.shields.io/github/commit-activity/m/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulse)
[![Open PRs](https://img.shields.io/github/issues-pr/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls)
[![Merged PRs](https://badgen.net/github/merged-prs/wnl-icb-analytics/dbt-analytics)](https://github.com/wnl-icb-analytics/dbt-analytics/pulls?q=is%3Amerged)
[![Test Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/EddieDavison92/fe9920551839b7a85d0f47dfd527e62b/raw/coverage.json)](https://github.com/wnl-icb-analytics/dbt-analytics/actions/workflows/test-coverage.yml)
[![License](https://img.shields.io/badge/license-OGL%20v3%20|%20MIT-blue)](LICENSE)

This public dbt project transforms WNL ICB healthcare data in Snowflake into
documented datasets for analysis, reporting, and direct care.

## Start here

- To learn dbt and this project, use the
  [dbt onboarding courses and handbook](https://dbt-onboarding.vercel.app/).
- To set up a development environment and open a pull request, read
  [Contributing](CONTRIBUTING.md).
- Before changing a model, read the
  [project conventions](PROJECT_CONVENTIONS.md).

## Quick start

```powershell
git clone https://github.com/wnl-icb-analytics/dbt-analytics
Set-Location dbt-analytics
Copy-Item env.example .env
.\start_dbt.ps1
dbt deps
dbt debug
```

Edit `.env` with your Snowflake connection details before you run `dbt debug`.
`start_dbt.ps1` installs the dbt Fusion engine and the project tooling. Python
and `uv` are only needed for helper scripts in `scripts/`.

For a cloud development environment, follow
[Developing in GitHub Codespaces](docs/codespaces.md).

## Project scope

The project covers two broad domains:

- Commissioning data, including secondary care, waiting lists, community
  services, and mental health services.
- OLIDS primary care data, including QOF registers, clinical programmes, and
  population health measures.

Sources include OLIDS, SUS, waiting lists, CSDS, MHSDS, EPD, and eRS.

## Model layout

```text
models/
├── sources/       # Source declarations
├── raw/           # Generated one-to-one source views
├── staging/       # Clean source interfaces
├── reference/     # Shared reference datasets
├── modelling/     # Reusable transformations and domain rules
├── reporting/     # Datasets for direct analysis
├── semantic/      # Snowflake semantic views
├── published/     # Governed datasets for named products
└── partner/       # Governed partner datasets and access rules
```

Source data follows one route: `DATA_LAKE` to raw to staging. Only staging
models may reference raw models. The
[project conventions](PROJECT_CONVENTIONS.md) define the contracts, naming,
testing, performance, and review rules for every model area.

## Project documentation

| Document | Purpose |
|---|---|
| [Project conventions](PROJECT_CONVENTIONS.md) | Model contracts, boundaries, SQL, tests, validation, and review rules |
| [Contributing](CONTRIBUTING.md) | Local setup, commit signing, branches, and pull requests |
| [GitHub Codespaces](docs/codespaces.md) | Cloud development setup and authentication |
| [GitHub Actions](docs/github-actions.md) | CI, deployment, and repository automation |
| [Working with sources](docs/working-with-sources.md) | Source declarations, raw model generation, and schema drift |
| [SLAM data](docs/slam-data-guide.md) | Source-to-staging guidance for SLAM contract feeds |
| [SUS models](docs/sus-models.md) | Secondary care model structure |

Older learning guides remain in [`docs/archive/`](docs/archive/). The onboarding
site replaces them for current training.

## Technology

- dbt Fusion runs dbt locally and in GitHub Actions.
- Snowflake hosts the data warehouse.
- Python 3.11 runs the helper scripts in `scripts/`.

## Licence

The project is dual-licensed under the Open Government Licence v3.0 and the MIT
Licence. Code outputs are subject to Crown copyright.
