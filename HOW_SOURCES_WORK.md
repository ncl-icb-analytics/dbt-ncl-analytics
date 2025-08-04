# How dbt Sources Work in This Project

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
  # No domain specified, but script knows this is commissioning
```

## Staging Model Distribution

When you run `3_generate_staging_models.py`, it reads `sources.yml` and creates staging models in the appropriate folders:

```
models/
├── sources.yml                    # ONE file with ALL sources
├── commissioning/
│   ├── staging/                  # stg_wl_*, stg_sus_op_*, stg_sus_apc_*, stg_epd_pc_*, stg_dictionary_*
│   ├── modelling/                # Business logic models
│   ├── reporting/                # Reporting layer
│   └── published_reporting_secondary_use/  # Published outputs
├── olids/
│   ├── staging/                  # (future integration)
│   ├── modelling/                # Clinical logic models
│   ├── reporting/                # Clinical reporting
│   ├── published_reporting_direct_care/     # Direct care outputs
│   └── published_reporting_secondary_use/   # Secondary use outputs
└── shared/
    ├── staging/                  # Cross-domain reference data staging
    ├── modelling/                # Shared dimensions, lookups
    ├── reporting/                # Cross-domain reports
    ├── published_reporting_direct_care/     # Shared direct care
    └── published_reporting_secondary_use/   # Shared secondary use
```

## Key Points

1. **One sources.yml** - All sources in one file
2. **Multiple staging folders** - Models distributed by domain
3. **source() function** - Works the same regardless of folder:
   ```sql
   -- In any staging model:
   SELECT * FROM {{ source('wl', 'WL_OpenPathways_Data') }}
   ```

4. **Domain selection** - Models are organised by domain for clear separation of concerns