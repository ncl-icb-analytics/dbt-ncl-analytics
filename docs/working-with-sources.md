# How dbt Sources Work in This Project

## Overview

This project uses a dynamic, configuration-driven approach to generate dbt sources and staging models. Everything is controlled by a single configuration file that drives the entire process.

## Source File Structure

Sources are organised into two types:

1. **Auto-generated sources** (`auto_*.yml` files):
   - Generated automatically from database metadata
   - Located in `models/sources/auto_*.yml`
   - Include all tables found in the configured database/schema
   - Re-generated each time the source generation script runs

2. **Manual sources** (`sources.yml`):
   - Manually defined and maintained
   - Located in `models/sources/sources.yml`
   - Used for sources not in `source_mappings.yml` or with custom table definitions
   - Take precedence over auto-generated sources (prevents duplicates)

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

### 4. Generate dbt Sources Files

Run the Python script to convert the CSV metadata into dbt sources:

```bash
python scripts/sources/2_generate_sources.py
```

This creates auto-generated source files (`models/sources/auto_*.yml`) for all sources defined in `source_mappings.yml`. 

**Important:** The script automatically:
- Skips auto-generating sources that are manually defined in `sources.yml`
- Cleans up existing auto-generated files if sources become manual
- Prevents duplicate source definitions

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

## Manual Sources (sources.yml)

**Location:** `models/sources/sources.yml`

This file contains manually defined sources that:
- Are NOT in `source_mappings.yml` (fully manual sources like `aic`)
- Override auto-generated sources with custom table definitions (like `c_ltcs`)

### When to Use Manual Sources

Use manual sources in `sources.yml` when:

1. **Source is not in source_mappings.yml**: The source won't be auto-generated, so define it manually
2. **You need custom table definitions**: Only specific tables are needed, not all tables in the schema
3. **You need custom column definitions**: Override auto-generated column metadata

### Examples

**Fully manual source** (not in source_mappings.yml):
```yaml
- name: aic
  database: '"DATA_LAKE__NCL"'
  schema: '"AIC_DEV"'
  description: AIC pipelines
  tables:
    - name: BASE_ATHENA__CONCEPT
      # ... column definitions
```

**Partial override** (source exists in source_mappings.yml but only some tables needed):
```yaml
- name: c_ltcs
  database: '"DEV__PUBLISHED_REPORTING__DIRECT_CARE"'
  schema: '"C_LTCS"'
  description: C-LTCS tables
  tables:
    - name: MDT_LOOKUP  # Only this table, not all tables in schema
      # ... column definitions
```

## Auto-Generated Sources

**Location:** `models/sources/auto_*.yml`

These files are automatically generated and include ALL tables found in the configured database/schema. Do not edit these files manually - they will be overwritten on the next generation run.

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

1. **Manual sources override auto-generated** - If a source exists in `sources.yml`, it won't be auto-generated
2. **No duplicates** - The generation script prevents duplicate source definitions
3. **Multiple staging folders** - Models distributed by domain
4. **source() function** - Works the same regardless of folder:
   ```sql
   -- In any staging model:
   SELECT * FROM {{ source('wl', 'WL_OpenPathways_Data') }}
   ```

5. **Domain selection** - Models are organised by domain for clear separation of concerns

## Best Practices

### Adding a New Source

1. **If all tables are needed**: Add to `source_mappings.yml` and let it auto-generate
2. **If only specific tables needed**: Add to `sources.yml` with manual table definitions
3. **If source not in mappings**: Define fully in `sources.yml`

### Updating an Existing Source

1. **Auto-generated source**: Update `source_mappings.yml` and regenerate
2. **Manual source**: Edit `sources.yml` directly
3. **Convert auto to manual**: Add to `sources.yml` and the script will skip auto-generation automatically

### Avoiding Duplicates

- Never define the same source in both `sources.yml` and auto-generated files
- The script prevents this automatically, but if you see duplicate errors:
  - Check if source exists in `sources.yml`
  - Check if source is being auto-generated
  - Remove it from one location