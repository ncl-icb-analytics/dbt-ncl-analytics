# How dbt Sources Work in This Project

## Overview

This project uses a dynamic, configuration-driven approach to generate dbt sources and staging models. Everything is controlled by a single configuration file that drives the entire process.

## Step-by-Step Workflow

### 1. Configure Your Data Sources

Edit `scripts/sources/source_mappings.yml` to define which databases and schemas you want to include:

```yaml
# Example configuration
- source_name: wl
  database: DATA_LAKE
  schema: WL
  description: Waiting lists and patient pathway data
  staging_prefix: stg_wl
  domain: commissioning
```

### 2. Generate Dynamic SQL Query

Run the Python script to create a custom SQL query based on your configuration:

```bash
python scripts/sources/1_generate_metadata_query.py
```

This creates `scripts/sources/metadata_query.sql` with SQL that queries only the databases/schemas you've configured.

### 3. Extract Metadata from Snowflake

1. Open `scripts/sources/metadata_query.sql`
2. Copy the entire SQL query
3. Paste into Snowflake UI and execute
4. Export results as CSV 
5. Save as `table_metadata.csv` in the project root directory

### 4. Generate dbt Sources File

Run the Python script to convert the CSV metadata into dbt sources:

```bash
python scripts/sources/2_generate_sources.py
```

This creates `models/sources.yml` with all your table definitions.

### 5. Generate Staging Models

Run the Python script to create individual staging SQL files:

```bash
python scripts/sources/3_generate_staging_models.py
```

This creates SQL files in `models/commissioning/staging/`, `models/olids/staging/`, etc.

### 6. Build Your dbt Models

```bash
dbt run         # Build all models
dbt test        # Run data quality tests
```

## Single sources.yml File

**Location:** `models/sources.yml`

This single file contains ALL data sources from ALL domains:
- Commissioning sources (wl, sus_op, sus_apc, epd_primary_care, dictionary)
- OLIDS sources (future integration when OLIDS moves out of UAT)
- Shared sources (cross-domain reference data)

## How Sources Map to Models

The `source_mappings.yml` configuration determines:
1. Which sources get included in `sources.yml`
2. Which folder the staging models go into

```yaml
# Example from source_mappings.yml
- source_name: wl
  database: DATA_LAKE
  schema: WL
  staging_prefix: stg_wl
  domain: commissioning  # Explicitly define the target domain
```

## Staging Model Distribution

When you run `3_generate_staging_models.py`, it reads `sources.yml` and creates staging models in the appropriate folders:

- `models/commissioning/staging/` - stg_wl_*, stg_sus_op_*, stg_sus_apc_*, stg_epd_pc_*
- `models/olids/staging/` - (future integration when OLIDS moves out of UAT)
- `models/shared/staging/` - stg_dictionary_* (reference/lookup data used across domains)

## Key Points

1. **One sources.yml** - All sources in one file
2. **Multiple staging folders** - Models distributed by domain
3. **source() function** - Works the same regardless of folder:
   ```sql
   -- In any staging model:
   SELECT * FROM {{ source('wl', 'WL_OpenPathways_Data') }}
   ```

4. **Domain selection** - Models are organised by domain for clear separation of concerns