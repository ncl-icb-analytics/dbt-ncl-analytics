# NCL Analytics DBT Project

## What This Is

dbt (data build tool) project for NCL Analytics supporting:
- **Commissioning analytics** - Secondary care and waiting lists
- **OLIDS analytics** - GP data from the One London Integrated Data Set

**Data sources:**
- **OLIDS** - FHIR GP record data (EMIS/SystmOne)
- **Waiting Lists (WL)** - Patient pathways and waiting times
- **SUS Unified** - Outpatient, Admitted Patient Care, Emergency Care
- **EPD Primary Care** - Medications and prescribing
- **eRS** - Electronic referral service
- **Dictionary** - Reference data and lookups

## Architecture

Based on the 3-database architecture:

```
DATA_LAKE → Staging (MODELLING.DBT_STAGING) → Intermediate (MODELLING.*) → Marts (REPORTING.*)
```

**Database Structure:**
- `DATA_LAKE.*` - Source data (WL, SUS_UNIFIED_OP, SUS_UNIFIED_APC, EPD_PRIMARY_CARE)
- `Dictionary.*` - Reference data and lookups (Dictionary schemas staged as required)
- `MODELLING.*` - Intermediate processing (DEV__ prefix for dev)
- `REPORTING.*` - Final marts (DEV__ prefix for dev) 

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Git
- Access to Snowflake with ANALYST role
- Windows PowerShell (for start_dbt.ps1 script)

### 1. Clone the repository

```bash
git clone https://github.com/ncl-icb-analytics/dbt-ncl-analytics
cd dbt-ncl-analytics
```

### 2. Set up Python environment

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Important:** This project uses dbt-core 1.9.4 (not 1.10+) for Snowflake compatibility. Snowflake's dbt runtime does not yet support dbt 1.10 test macro syntax. Do not upgrade dbt packages or use the new `arguments:` property in test definitions.

**Note on VSCode warnings:** If using the dbt VSCode extension, you may see deprecation warnings (dbt0102) about test arguments. These are cosmetic warnings from the extension's parser and can be safely ignored. The code is correct for dbt 1.9.4 and Snowflake.

*Note: If `python` command fails, use `py -m venv venv` instead. To add Python to PATH without admin rights, find where Python is installed (run `py -c "import sys; print(sys.executable)"` to locate it), then run in PowerShell:*
```powershell
[Environment]::SetEnvironmentVariable("PATH", "$env:PATH;C:\Path\To\Python", "User")
```
*Replace `C:\Path\To\Python` with your Python installation directory. Restart PowerShell after running this command.*

### 3. Configure Snowflake connection

Two configuration files are needed:

**Environment file (.env)** - Contains Snowflake account details:
```bash
cp env.example .env
```
Edit `.env` with your Snowflake account, username, warehouse, and role (ANALYST).

**dbt profile (profiles.yml)** - Configures how dbt connects to Snowflake:
```bash
cp profiles.yml.template profiles.yml
```
Edit `profiles.yml` with your username and authentication method (typically externalbrowser for SSO).

### 4. Initialise development environment

```powershell
.\start_dbt.ps1
```

This script:
- Loads .env variables into your session
- Applies git skip-worktree to profiles.yml (allows your local profile to diverge from the repo)

Run once before your first commit, then each new terminal session for environment variables.

**Why skip-worktree instead of gitignore?** profiles.yml must be committed for Snowflake native execution, but each developer needs their own local configuration. gitignore doesn't work for files already tracked by git - skip-worktree tells git to ignore changes to an already-tracked file.

### 5. Verify installation

```bash
dbt deps
dbt debug
```

Your browser will open for Snowflake authentication. Look for "All checks passed!"

## Setting Up Data Sources

1. **Configure sources**: Edit `scripts/sources/source_mappings.yml`
2. **Generate metadata query**: `python scripts/sources/1a_generate_metadata_query.py`
3. **Extract metadata**: `python scripts/sources/1b_extract_metadata.py`
4. **Generate sources.yml**: `python scripts/sources/2_generate_sources.py`
5. **Generate staging models**: `python scripts/sources/3_generate_staging_models.py`
6. **Build and test**: `dbt run && dbt test`

## Data Sources

| Source | Staging Models | Description |
|--------|---------------|-------------|
| Waiting Lists (WL) | `stg_wl_*` | Patient pathways and waiting times |
| SUS Unified OP | `stg_sus_op_*` | Outpatient appointments |
| SUS Unified APC | `stg_sus_apc_*` | Admitted patient care |
| SUS Unified ECDS | `stg_sus_ecds_*` | Emergency care |
| EPD Primary Care | `stg_epd_pc_*` | Prescribing and medications |
| eRS Primary Care | `stg_ers_pc_*` | Electronic referrals |
| Dictionary | `stg_dictionary_*` | Reference data and lookups |

## Project Structure

```
models/
├── commissioning/           # Commissioning analytics domain
│   ├── staging/             # 1:1 source mappings (views in MODELLING.DBT_STAGING)
│   ├── modelling/           # Business logic & consolidation (tables in MODELLING.COMMISSIONING_MODELLING)
│   ├── reporting/           # Analytics-ready models (tables in REPORTING.COMMISSIONING_REPORTING)
│   └── published_reporting_secondary_use/  # Published outputs (PUBLISHED_REPORTING__SECONDARY_USE.COMMISSIONING_PUBLISHED)
│
├── olids/                   # OLIDS analytics domain
│   ├── staging/             # 1:1 source mappings (views in MODELLING.DBT_STAGING)
│   ├── modelling/           # Business logic & consolidation (tables in MODELLING.OLIDS_MODELLING)
│   ├── reporting/           # Analytics-ready models (tables in REPORTING.OLIDS_REPORTING)
│   ├── published_reporting_direct_care/     # Direct care outputs (PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_PUBLISHED)
│   └── published_reporting_secondary_use/   # Secondary use outputs (PUBLISHED_REPORTING__SECONDARY_USE.OLIDS_PUBLISHED)
│
└── shared/                  # Shared reference data and utilities
    ├── staging/             # Reference data staging (views in MODELLING.DBT_STAGING)
    ├── modelling/           # Shared dimensions and lookups (tables in MODELLING.REFERENCE)
    ├── reporting/           # Shared reporting models (tables in REPORTING.REFERENCE)
    ├── published_reporting_direct_care/     # Shared direct care outputs (PUBLISHED_REPORTING__DIRECT_CARE.REFERENCE)
    └── published_reporting_secondary_use/   # Shared secondary use outputs (PUBLISHED_REPORTING__SECONDARY_USE.REFERENCE)

scripts/                     # Automation utilities
└── sources/                     # Source and staging setup scripts
    ├── source_mappings.yml          # Configuration: Define data sources and mappings
    ├── 1_generate_metadata_query.py # Step 1: Generate dynamic SQL from source mappings
    ├── 2_generate_sources.py        # Step 2: Generate sources.yml from metadata CSV
    └── 3_generate_staging_models.py # Step 3: Create staging SQL files

macros/                      # Reusable SQL macros
├── generate_database_name.sql   # Handle DEV__ database prefixes
├── generate_schema_name.sql     # Schema name generation
├── add_model_comment.sql        # Add metadata comments to models
└── generate_table_comment.sql   # Generate model comment content
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines, branch protection rules, and commit signing setup.

**Daily workflow:**
```bash
dbt run            # Build all models
dbt test           # Run data quality tests
dbt docs generate  # Generate documentation
dbt docs serve     # Open documentation in browser
```

**For specific models:**
```bash
dbt run -s model_name              # Build one model
dbt run -s staging                 # Build all staging models
dbt run -s +model_name             # Build model + dependencies
```

**For specific domains:**
```bash
dbt run -s commissioning           # Build all commissioning models
dbt run -s olids                   # Build all OLIDS models
dbt run -s shared                  # Build all shared models
dbt run -s commissioning.staging   # Build only commissioning staging models
```

**Generate YML outline:**
```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["your-model-name-here",], "upstream_descriptions": true}'
```

## Schema and Database Generation

Custom macros override dbt defaults. All locations are configured in `dbt_project.yml`.

**Database naming**:
- **Prod**: Base database name (e.g., `REPORTING.COMMISSIONING_REPORTING.model`)
- **Dev**: Prefixed with `DEV__` (e.g., `DEV__REPORTING.COMMISSIONING_REPORTING.model`)

**Schema naming**:
- Uses exact `+schema:` value from config (no target prefix added)
- Example: `COMMISSIONING_REPORTING`, `OLIDS_DIAGNOSES`

When adding folders, update `dbt_project.yml` with `+database:` and `+schema:` settings. Unconfigured models default to `MODELLING.DBT_STAGING`.

## Role and Permissions

This project primarily uses the **ANALYST** role, which has access to:
- `DATA_LAKE.*` - Source data
- `Dictionary.*` - Reference data
- `MODELLING.*` - Intermediate processing
- `REPORTING.*` - Final marts
- `PUBLISHED_REPORTING__SECONDARY_USE.*` - Published outputs
- `PUBLISHED_REPORTING__DIRECT_CARE.*` - Direct care outputs

**Role hierarchy**:
- **ANALYST** - Base role, owns all dbt-created objects
- **ENGINEER** - Inherits ANALYST permissions
- **DATA_PLATFORM_MANAGER** - Inherits ENGINEER permissions

Models can be run using any of these roles. Ownership is automatically transferred to ANALYST with `COPY CURRENT GRANTS`, meaning ANALYST becomes the owner while ENGINEER and DATA_PLATFORM_MANAGER retain management access through role inheritance.

