# Development Guide

This guide covers advanced development patterns and technical details for working with the NCL Analytics dbt project.

## Daily Development Workflow

### Running Models

**Build all models:**
```bash
dbt run            # Build all models
dbt test           # Run data quality tests
```

**Build specific models:**
```bash
dbt run -s model_name              # Build one model
dbt run -s +model_name             # Build model + upstream dependencies
dbt run -s model_name+             # Build model + downstream dependents
dbt run -s +model_name+            # Build model + all dependencies
```

**Build by folder/domain:**
```bash
dbt run -s staging                 # Build all staging models
dbt run -s commissioning           # Build all commissioning models
dbt run -s olids                   # Build all OLIDS models
dbt run -s commissioning.staging   # Build only commissioning staging models
```

### Testing and Documentation

```bash
dbt test                           # Run all tests
dbt test -s model_name             # Test specific model
dbt docs generate                  # Generate documentation
dbt docs serve                     # Open documentation in browser
```

### Generating Model YAML

Use the codegen package to generate YAML outlines:

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["your-model-name"], "upstream_descriptions": true}'
```

## Project Architecture

### Layer-Based Structure

The project follows a medallion architecture with clear separation between layers:

```
models/
├── raw/                     # Source data with minimal transformation
│   ├── commissioning/       # Commissioning source data (views in MODELLING.DBT_RAW)
│   ├── olids/               # OLIDS source data (views in MODELLING.DBT_RAW)
│   ├── phenolab/            # Phenotype lab data (views in MODELLING.DBT_RAW)
│   └── shared/              # Shared reference data (views in MODELLING.DBT_RAW)
│
├── staging/                 # Cleaned and standardised source data
│   ├── commissioning/       # 1:1 source mappings (views in MODELLING.DBT_STAGING)
│   ├── olids/               # 1:1 OLIDS mappings (views in MODELLING.DBT_STAGING)
│   └── shared/              # Reference data staging (views in MODELLING.DBT_STAGING)
│
├── modelling/               # Business logic and transformations
│   ├── commissioning/       # Commissioning intermediate models (subdomain schemas)
│   └── olids/               # OLIDS intermediate models (subdomain schemas auto-generated)
│
├── reporting/               # Aggregated and analytical models
│   ├── commissioning/       # Commissioning reporting (subdomain schemas)
│   └── olids/               # OLIDS reporting (subdomain schemas auto-generated)
│
└── published/               # Production-ready datasets for end users
    ├── direct_care/         # Direct care outputs
    └── secondary_use/       # Secondary use outputs
```

### Domain Organisation

Models are organised by domain (commissioning, olids, shared) with subdomain folders for specific functional areas:

**Commissioning subdomains:**
- `administrative/` - Administrative and organisational data
- `diagnosis/` - Diagnostic codes and disease groupings
- `encounters/` - Patient encounters and activity
- `observations/` - Clinical observations and measurements
- `procedure/` - Procedures and interventions

**OLIDS subdomains:**
- `diagnoses/` - QOF disease registers and clinical diagnoses
- `medications/` - Prescribing and medication management
- `observations/` - Clinical observations and test results
- `organisation/` - Practice and organisational hierarchies
- `person_attributes/` - Patient demographics and registration
- `programme/` - Clinical programmes (immunisations, screening, etc.)
- `utilities/` - Helper models and calculations

## Schema and Database Generation

Custom macros override dbt defaults to provide flexible naming:

### Database Naming

Configured via `generate_database_name` macro:

- **Production**: Base database name (e.g., `REPORTING`)
- **Development**: Prefixed with `DEV__` (e.g., `DEV__REPORTING`)

The macro automatically detects your target and applies the appropriate prefix.

### Schema Naming

Configured via `generate_schema_name` macro with two approaches:

**1. Explicit schemas** (default):
- Uses exact `+schema:` value from config
- No target prefix added
- Example: `COMMISSIONING_REPORTING`, `REFERENCE`

**2. Automatic schemas** (for configured domains):
- Schema names automatically derived from subdomain folder structure
- Pattern: `{DOMAIN}_{SUBDOMAIN}`
- Example: `models/modelling/olids/diagnoses/` → `OLIDS_DIAGNOSES`
- Configure via `vars.auto_schema_domains` in `dbt_project.yml`
- Currently enabled for: `olids`

### Adding New Folders

**For domains using automatic schemas:**
Simply create the subdomain folder - no config changes needed. The schema name will be automatically generated.

**For other domains:**
Update `dbt_project.yml` with appropriate `+database:` and `+schema:` settings:

```yaml
models:
  your_project:
    modelling:
      your_domain:
        +database: MODELLING
        +schema: YOUR_DOMAIN_MODELLING
        your_subdomain:
          +schema: YOUR_DOMAIN_YOUR_SUBDOMAIN
```

**Unconfigured models** default to `MODELLING.DBT_STAGING`.

### Full Object Naming Examples

| Target | Config | Folder Path | Database | Schema | Result |
|--------|--------|-------------|----------|--------|--------|
| dev | `+database: REPORTING`<br>`+schema: COMMISSIONING_REPORTING` | `reporting/commissioning/` | `DEV__REPORTING` | `COMMISSIONING_REPORTING` | `DEV__REPORTING.COMMISSIONING_REPORTING.model` |
| prod | `+database: REPORTING`<br>`+schema: COMMISSIONING_REPORTING` | `reporting/commissioning/` | `REPORTING` | `COMMISSIONING_REPORTING` | `REPORTING.COMMISSIONING_REPORTING.model` |
| dev | Auto-schema enabled | `modelling/olids/diagnoses/` | `DEV__MODELLING` | `OLIDS_DIAGNOSES` | `DEV__MODELLING.OLIDS_DIAGNOSES.model` |
| prod | Auto-schema enabled | `modelling/olids/diagnoses/` | `MODELLING` | `OLIDS_DIAGNOSES` | `MODELLING.OLIDS_DIAGNOSES.model` |

## Roles and Permissions

### Primary Role: ANALYST

This project uses the **ANALYST** role, which has access to:
- `DATA_LAKE.*` - Source data
- `Dictionary.*` - Reference data
- `MODELLING.*` - Intermediate processing
- `REPORTING.*` - Final marts
- `PUBLISHED_REPORTING__SECONDARY_USE.*` - Published outputs
- `PUBLISHED_REPORTING__DIRECT_CARE.*` - Direct care outputs

### Role Hierarchy

```
ANALYST (base role, owns all dbt-created objects)
  ↓
ENGINEER (inherits ANALYST permissions)
  ↓
DATA_PLATFORM_MANAGER (inherits ENGINEER permissions)
```

Models can be run using any of these roles. Ownership is automatically transferred to ANALYST with `COPY CURRENT GRANTS`, meaning ANALYST becomes the owner while ENGINEER and DATA_PLATFORM_MANAGER retain management access through role inheritance.

## Custom Macros

### Model Comments

The `add_model_comment` and `generate_table_comment` macros automatically add metadata comments to all models:

```sql
-- Automatically added to all models
-- Domain: olids
-- Layer: reporting
-- Updated: 2025-10-26
```

These comments help with discoverability and understanding model purpose in Snowflake.

### Other Macros

Explore the `macros/` directory for additional utilities:
- Date and time calculations
- String manipulation
- Custom aggregations
- Testing helpers

## dbt Packages

This project uses several dbt packages (committed to `dbt_packages/` for Snowflake native execution):

- **dbt_utils** - Common utilities and macros
- **dbt_expectations** - Data quality tests
- **dbt_date** - Date manipulation helpers
- **codegen** - Code generation utilities

**Important**: This project uses dbt-core 1.9.4 for Snowflake compatibility. Do not upgrade to dbt 1.10+ or use the new `arguments:` property in test definitions.

## Working with Profiles

The `profiles.yml` file is committed for Snowflake native execution but uses `git skip-worktree` to prevent committing local credentials.

The `start_dbt.ps1` script manages this automatically. If you need to manually manage skip-worktree:

```bash
# Enable skip-worktree (ignore local changes)
git update-index --skip-worktree profiles.yml

# Disable skip-worktree (track changes again)
git update-index --no-skip-worktree profiles.yml

# Check skip-worktree status
git ls-files -v | grep "^S"
```

## Troubleshooting

### Common Issues

**Import errors when running Python scripts:**
```bash
# Ensure virtual environment is activated
venv\Scripts\activate
```

**Authentication failures:**
```bash
# Test connection
dbt debug

# Check credentials in .env
cat .env
```

**Models building to wrong schema:**
- Check `dbt_project.yml` configuration
- Verify folder structure matches expected patterns
- Check for explicit `+schema:` config in model files

**Skip-worktree not working:**
```bash
# Re-run setup script
.\start_dbt.ps1
```

### Getting Help

- Check [GitHub Issues](https://github.com/ncl-icb-analytics/dbt-ncl-analytics/issues)
- Review dbt error messages carefully
- Use `dbt compile` to see generated SQL
- Check `target/compiled/` and `target/run/` for debugging
