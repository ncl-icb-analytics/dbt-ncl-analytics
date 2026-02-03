# Configuration Macros

This directory contains centralised configuration macros that were previously scattered across `dbt_project.yml` vars.

## Why Macros Instead of Vars?

1. **Documentation**: Macros can have detailed docstrings explaining each setting
2. **Defaults**: Default values are clearly visible in the macro definition
3. **Discoverability**: All campaign/config settings are in one searchable place
4. **Override flexibility**: Can still be overridden via `--vars` when needed

## Files

| File | Purpose |
|------|---------|
| `flu_campaign_selection.sql` | Current/previous flu campaign identifiers |
| `covid_campaign_selection.sql` | Current/previous COVID campaign identifiers (autumn/spring) |
| `qof_config.sql` | QOF reference date and related settings |
| `schema_config.sql` | Auto-schema generation domains, audit schema |
| `elementary_config.sql` | Elementary observability package settings |

## Usage Examples

### In Models

```sql
-- Instead of: var('flu_current_campaign', 'Flu 2024-25')
-- Use:
{{ flu_campaign_config(get_flu_current_campaign()) }}

-- QOF reference date
WHERE clinical_effective_date <= {{ get_qof_reference_date() }}
```

### Override at Runtime

All macros still support runtime override via `--vars`:

```bash
dbt run --vars '{"flu_current_campaign": "Flu 2024-25"}'
dbt run --vars '{"qof_reference_date": "2025-03-31"}'
```

## Changing Campaign Years

To switch to a new campaign year:

1. Update the default value in the relevant macro (e.g., `flu_campaign_selection.sql`)
2. Add any new campaign definitions to `campaigns/flu_campaign_config.sql` or `campaigns/covid_campaign_config.sql`
3. Run `dbt run` - all downstream models will use the new campaign
