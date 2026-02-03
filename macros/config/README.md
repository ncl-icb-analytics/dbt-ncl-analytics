# Configuration Macros

Centralised configuration for campaigns, QOF, and observability settings.

## Quick Reference

| Macro | Returns | Example |
|-------|---------|---------|
| `flu_current_config()` | Full campaign config CTE | `SELECT * FROM ({{ flu_current_config() }})` |
| `flu_previous_config()` | Previous campaign config CTE | `SELECT * FROM ({{ flu_previous_config() }})` |
| `covid_autumn_config()` | Current autumn config CTE | `SELECT * FROM ({{ covid_autumn_config() }})` |
| `covid_spring_config()` | Current spring config CTE | `SELECT * FROM ({{ covid_spring_config() }})` |
| `covid_previous_autumn_config()` | Previous autumn config CTE | `SELECT * FROM ({{ covid_previous_autumn_config() }})` |
| `qof_reference_date()` | Reference date for QOF | `WHERE date <= {{ qof_reference_date() }}` |

## Usage in Models

```sql
WITH all_campaigns AS (
    SELECT * FROM ({{ flu_current_config() }})
    UNION ALL
    SELECT * FROM ({{ flu_previous_config() }})
),
...
```

## Override at Runtime

```bash
dbt run --vars '{"flu_current_campaign": "Flu 2024-25"}'
dbt run --vars '{"qof_reference_date": "2025-03-31"}'
```

## Changing Campaign Years

1. Update defaults in `flu_campaign_selection.sql` or `covid_campaign_selection.sql`
2. Add new campaign definitions in `campaigns/flu_campaign_config.sql` if needed
3. Run `dbt run`
