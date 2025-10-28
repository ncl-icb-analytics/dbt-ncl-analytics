# NCL Analytics dbt Project

dbt (data build tool) project for NCL ICB Analytics, transforming healthcare data into actionable insights across North Central London.

## What We Build

This project transforms healthcare data into analytical datasets across two main domains:

**Commissioning analytics** - Secondary care activity, waiting lists, community and mental health services

**OLIDS analytics** - QOF disease registers, clinical programmes, population health metrics

Data sources include OLIDS (GP data via FHIR), SUS Unified (secondary care), Waiting Lists, Community Services (CSDS/MHSDS), EPD Primary Care (prescribing), eRS (referrals), and Dictionary (reference data).

## Getting Started

**New to the project?** See [CONTRIBUTING.md](CONTRIBUTING.md) for complete setup instructions.

**Already set up?** See [Development Guide](docs/development-guide.md) for daily workflows and advanced patterns.

## Architecture

### Data Flow

```
DATA_LAKE → Raw → Staging → Modelling → Reporting → Published
           (views) (views)   (tables)    (tables)    (tables)
```

### Layer Organisation

```
models/
├── raw/           # 1:1 views of source data
├── staging/       # Cleaned and standardised source mappings
├── modelling/     # Modular transformations and building blocks
│   ├── commissioning/
│   ├── olids/
│   └── shared/
├── reporting/     # Analytics-ready datasets
│   ├── commissioning/
│   ├── olids/
│   └── shared/
└── published/     # Objects feeding external reports and dashboards
    ├── direct_care/
    └── secondary_use/
```

Each domain is further organised into subdomains (e.g., `diagnoses/`, `medications/`, `observations/`) with automatic schema generation for configured domains.

### Database Layers

- **DATA_LAKE** - Raw data repository with 1:1 views of external sources
- **MODELLING** - Initial transformations: filter, reshape, categorise, and link data sources
- **REPORTING** - Analytics-ready datasets with business metrics and KPIs
- **PUBLISHED_REPORTING__SECONDARY_USE** - Standard reporting layer for population health and operational analytics
- **PUBLISHED_REPORTING__DIRECT_CARE** - Restricted layer for individual patient care (consent-based access)

Development uses DEV__ prefixed variants (e.g., DEV__MODELLING, DEV__REPORTING) for safe development before promotion to production.

## Documentation

- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Complete onboarding for new contributors
- **[docs/development-guide.md](docs/development-guide.md)** - Daily workflows, commands, and advanced patterns
- **[docs/working-with-sources.md](docs/working-with-sources.md)** - Source generation workflow
- **[CHANGELOG.md](CHANGELOG.md)** - Release history

## Key Features

- **Configuration-driven sources** - Automated generation of dbt sources and staging models
- **Automatic schema generation** - Schemas derived from folder structure for configured domains
- **Conventional commits** - Semantic versioning with automated releases
- **Branch protection** - Signed commits required, all changes via pull requests
- **Comprehensive testing** - dbt tests and expectations for data quality

## Common Commands

```bash
dbt run                    # Build all models
dbt test                   # Run data quality tests
dbt run -s olids           # Build all OLIDS models
dbt run -s +model_name     # Build model with dependencies
dbt docs generate          # Generate documentation
dbt docs serve             # View documentation
```

See [Development Guide](docs/development-guide.md) for more commands and patterns.

## Technology Stack

- **dbt-core 1.9.4** - Data transformation framework (do not upgrade to 1.10+)
- **Snowflake** - Cloud data warehouse
- **Python 3.8+** - Scripting and automation
- **dbt packages** - dbt_utils, dbt_expectations, dbt_date, codegen

## Roles and Permissions

Uses **ANALYST** role with access to all project databases. Role hierarchy: ANALYST → ENGINEER → DATA_PLATFORM_MANAGER. All dbt objects owned by ANALYST with inherited permissions.

## Release Management

Automated semantic versioning with [release-please](https://github.com/googleapis/release-please):
- Releases created from conventional commit messages
- Changelog automatically generated
- Release PRs auto-merge when created

## Getting Help

- **New contributor?** [CONTRIBUTING.md](CONTRIBUTING.md)
- **Need workflows?** [Development Guide](docs/development-guide.md)
- **Working with sources?** [Working with Sources](docs/working-with-sources.md)
- **Found a bug?** [Open an issue](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/issues)

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.
