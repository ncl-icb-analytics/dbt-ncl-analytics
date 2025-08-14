# NCL Analytics DBT Project

## What This Is

**dbt** (data build tool) project for NCL Analytics supporting both:
- **Commissioning analytics** - Currently active using ANALYST role
- **OLIDS analytics** - Future integration when OLIDS moves out of UAT

This project uses dbt to transform healthcare and operational data, creating analytics-ready datasets for analysis from multiple data sources:

**Data sources included:**
- **Waiting Lists (WL)** - Patient waiting times and pathway data  
- **SUS Unified** - Outpatient (OP) and Admitted Patient Care (APC) data
- **EPD Primary Care** - Primary care medications and prescribing data
- **Dictionary** - Reference data and lookup tables (shared across domains)

## Architecture

Based on the 3-database architecture:

```
DATA_LAKE → Staging (MODELLING.DBT_STAGING) → Intermediate (MODELLING.*) → Marts (REPORTING.*)
```

**Database Structure:**
- `DATA_LAKE.*` - Source data (WL, SUS_UNIFIED_OP, SUS_UNIFIED_APC, EPD_PRIMARY_CARE)
- `Dictionary.dbo` - Reference data and lookups
- `MODELLING.*` - Intermediate processing (DEV__ prefix for dev)
- `REPORTING.*` - Final marts (DEV__ prefix for dev) 

## Quick Start

**Prerequisites:** Python 3.8+, access to Snowflake with ANALYST role

```bash
# 1. Get the code
git clone https://github.com/ncl-icb-analytics/dbt-ncl-analytics
cd dbt-ncl-analytics

# 2. Setup Python environment
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt

# 3. Configure Snowflake connection
cp env.example .env
# Edit .env file with your Snowflake credentials (ANALYST role)

# 4. Setup profile
cp profiles.yml.template profiles.yml
# Edit profiles.yml as needed

# 5. Run environment setup script
.\start_dbt.ps1

# 6. Install dbt dependencies and test connection
dbt deps
dbt debug
```

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
python scripts\sources\1b_extract_metadata.py
# This creates table_metadata.csv
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

### EPD Primary Care (EPD_PRIMARY_CARE)
- Primary care prescribing and medications
- Staging models: `stg_epd_pc_*`

### Dictionary (dbo)
- Reference data and lookup tables (shared across domains)
- Staging models: `stg_dictionary_*` (in shared/staging/)

## Project Structure

```
models/
├── commissioning/           # Commissioning analytics domain
│   ├── staging/             # 1:1 source mappings (views in MODELLING.DBT_STAGING)
│   ├── modelling/           # Business logic & consolidation (tables in MODELLING.COMMISSIONING_MODELLING)
│   ├── reporting/           # Analytics-ready models (tables in REPORTING.COMMISSIONING_REPORTING)
│   └── published_reporting_secondary_use/  # Published outputs (PUBLISHED_REPORTING__SECONDARY_USE.COMMISSIONING_PUBLISHED)
│
├── olids/                   # OLIDS analytics domain (future)
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

**Daily workflow:**
```bash
dbt run         # Build all models
dbt test        # Run data quality tests  
dbt docs serve  # Open documentation
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
dbt run -s olids                   # Build all OLIDS models (when available)
dbt run -s shared                  # Build all shared models
dbt run -s commissioning.staging   # Build only commissioning staging models
```

## Environment Handling

- **Dev**: Models built in `DEV__MODELLING.*`, `DEV__REPORTING.*`, and `DEV__PUBLISHED_REPORTING__SECONDARY_USE.*`
- **Prod**: Models built in `MODELLING.*`, `REPORTING.*`, and `PUBLISHED_REPORTING__SECONDARY_USE.*`
- Handled automatically via `generate_database_name()` macro

## Role and Permissions

This project uses the **ANALYST** role which has access to:
- `DATA_LAKE.*` databases for source data
- `Dictionary.dbo` for reference data  
- `MODELLING.*` for intermediate processing
- `REPORTING.*` for final marts

## Future Integration

This project is designed to be merged with the OLIDS dbt project once OLIDS data moves out of the UAT environment requiring special role access.