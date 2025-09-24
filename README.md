# NCL Analytics DBT Project

## What This Is

**dbt** (data build tool) project for NCL Analytics supporting both:
- **Commissioning analytics** - Currently active using ANALYST role
- **OLIDS analytics** - GP data in the One London Integrated Data Set

This project uses dbt to transform healthcare and operational data, creating analytics-ready datasets for analysis from multiple data sources:

**Data sources included:**
- **OLIDS** - One London Integrated Data Set FHIR GP record data from EMIS/SystmOne.
- **Waiting Lists (WL)** - Patient waiting times and pathway data  
- **SUS Unified** - Outpatient (OP), Admitted Patient Care (APC) and Emergency Care Dataset (ECDS) data
- **EPD Primary Care** - Primary care medications and prescribing data
- **eRS electronic Referral Service** - Primary care referrals data
- **Dictionary** - Reference data and lookup tables (shared across domains)

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

Run the setup script to configure your environment:

```powershell
.\start_dbt.ps1
```

The start_dbt.ps1 script sets up your local development environment by loading your Snowflake credentials from .env and protecting your local profiles.yml changes from being committed.

Specifically, it:
- Loads your .env variables into the session
- Applies git skip-worktree to profiles.yml (a permanent local git config that prevents your credentials from being tracked)
- Sets up the dbt environment for development

**Important**: Unlike typical dbt projects, both profiles.yml and dbt_packages/ are committed to this repo for Snowflake native execution. The git skip-worktree setting is persistent across sessions and branches - you only need to run this once before your first commit, then again each new terminal session for the environment variables.

Note: Run this script each time you start a new terminal session (for env vars) and always before your first commit (for git skip-worktree).

### 5. Verify installation

Install dbt packages and test your connection:

```bash
dbt deps
dbt debug
```

When running `dbt debug`, your browser will open for Snowflake authentication. Once authenticated, you should see "All checks passed!" confirming your setup is complete.

## Setting Up Data Sources

### Step 1: Configure Data Sources

Edit `scripts/sources/source_mappings.yml` to define your data sources and mappings.

### Step 2: Extract & Download Schema Metadata

Generate dynamic SQL query based on your source mappings:

```bash
# Generate dynamic SQL query from your source mappings
python scripts/sources/1a_generate_metadata_query.py
# This creates scripts/sources/metadata_query.sql
```

```bash
# Generate dynamic SQL query from your source mappings
python scripts\\sources\\1b_extract_metadata.py
# This creates table_metadata.csv and saves it locally
```

### Step 3: Generate dbt Sources

```bash
python scripts/sources/2_generate_sources.py
# This creates models/sources.yml with all table definitions
```

### Step 4: Generate Staging Models

```bash
python scripts/sources/3_generate_staging_models.py
# This creates SQL files in models/*/staging/ directories
```

### Step 5: Build and Test

```bash
dbt run         # Builds all models
dbt test        # Runs data quality tests
```

## Data Sources

### Waiting Lists (WL)
- Patient pathways and waiting times
- Staging models: `stg_wl_*`

### SUS Unified - Outpatient (SUS_UNIFIED_OP) 
- Outpatient appointments and activity
- Staging models: `stg_sus_op_*`

### SUS Unified - Admitted Patient Care (SUS_UNIFIED_APC)
- Inpatient episodes and procedures  
- Staging models: `stg_sus_apc_*`

### SUS Unified - EMERGENCY CARE DATASET (SUS_UNIFIED_ECDS)
- Emergency care episodes and procedures  
- Staging models: `stg_sus_ecds_*`

### EPD Primary Care (EPD_PRIMARY_CARE)
- Primary care prescribing and medications
- Staging models: `stg_epd_pc_*`

### eRS Primary Care (eRS_PRIMARY_CARE)
- electronic referral system data for primary care referrals to outpatient services and first appointment bookings
- Staging models: `stg_ers_pc_*`

### Dictionary
- Reference data and lookup tables (shared across domains)
- Schemas staged as required
- Staging models: `stg_dictionary_*_*` (in shared/staging/) e.g., stg_dictionary_dbo_*

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

**For faster YML development:**
Print a YML outline into the terminal to paste into a new .yml. Descriptions will still need to be added.
```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["your-model-name-here",], "upstream_descriptions": true}'  
```

## Environment Handling

- **Dev**: Models built in `DEV__MODELLING.*`, `DEV__REPORTING.*`, and `DEV__PUBLISHED_REPORTING__SECONDARY_USE.*`
- **Prod**: Models built in `MODELLING.*`, `REPORTING.*`, and `PUBLISHED_REPORTING__SECONDARY_USE.*`
- Handled automatically via `generate_database_name()` macro

## Role and Permissions

This project uses the **ANALYST** role which has access to:
- `DATA_LAKE.*` databases for source data
- `Dictionary.*` for reference data  
- `MODELLING.*` for intermediate processing
- `REPORTING.*` for final marts

