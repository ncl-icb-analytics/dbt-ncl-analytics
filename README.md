# NCL Analytics dbt Project

dbt (data build tool) project for NCL ICB Analytics, transforming healthcare data into actionable insights across North Central London.

## What We Build

**Analytics domains:**
- **Commissioning analytics** - Secondary care activity, waiting lists, community and mental health services
- **OLIDS analytics** - QOF disease registers, clinical programmes, population health metrics

**Data sources:**
- OLIDS (GP data via FHIR), SUS Unified (secondary care), Waiting Lists, Community Services (CSDS/MHSDS), EPD Primary Care (prescribing), eRS (referrals), Dictionary (reference data)

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
├── raw/           # Source data with minimal transformation
├── staging/       # Cleaned 1:1 source mappings
├── modelling/     # Business logic and transformations
│   ├── commissioning/
│   ├── olids/
│   └── shared/
├── reporting/     # Aggregated analytical models
│   ├── commissioning/
│   ├── olids/
│   └── shared/
└── published/     # Production datasets
    ├── direct_care/
    └── secondary_use/
```

Each domain is further organised into subdomains (e.g., `diagnoses/`, `medications/`, `observations/`) with automatic schema generation for configured domains.

### Database Structure

- **DATA_LAKE.\*** - Source data
- **Dictionary.\*** - Reference data
- **MODELLING.\*** - Intermediate processing (DEV__ prefix in development)
- **REPORTING.\*** - Analytical marts (DEV__ prefix in development)
- **PUBLISHED_REPORTING__\*** - Production-ready outputs

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
