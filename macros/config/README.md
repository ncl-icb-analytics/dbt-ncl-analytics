# Configuration Macros

Centralised configuration for campaigns, QOF, and observability settings.

## Quick Reference

### Flu Campaign
```sql
-- Date accessors
{{ flu_current_campaign_start_date() }}
{{ flu_current_campaign_end_date() }}
{{ flu_current_campaign_reference_date() }}
{{ flu_current_child_reference_date() }}
{{ flu_current_vaccination_after_date() }}

-- Previous campaign variants
{{ flu_previous_campaign_start_date() }}
{{ flu_previous_campaign_end_date() }}
-- etc.

-- Campaign ID (string)
{{ flu_current_campaign() }}   -- 'Flu 2025-26'
{{ flu_previous_campaign() }}  -- 'Flu 2024-25'
```

### COVID Campaign
```sql
-- Autumn campaign
{{ covid_autumn_campaign_start_date() }}
{{ covid_autumn_campaign_end_date() }}
{{ covid_autumn_campaign_reference_date() }}

-- Spring campaign
{{ covid_spring_campaign_start_date() }}
{{ covid_spring_campaign_end_date() }}

-- Previous autumn
{{ covid_previous_autumn_campaign_start_date() }}
-- etc.
```

### QOF
```sql
{{ qof_reference_date() }}
```

## Usage Examples

```sql
-- Flu eligibility
WHERE birth_date <= DATEADD('year', -65, {{ flu_current_campaign_reference_date() }})

-- COVID vaccination window
WHERE vaccination_date >= {{ covid_autumn_vaccination_tracking_start() }}
  AND vaccination_date <= {{ covid_autumn_vaccination_tracking_end() }}

-- QOF register
WHERE clinical_effective_date <= {{ qof_reference_date() }}
```

## Override at Runtime

```bash
dbt run --vars '{"flu_current_campaign": "Flu 2024-25"}'
dbt run --vars '{"covid_current_autumn": "COVID Autumn 2024"}'
dbt run --vars '{"qof_reference_date": "2025-03-31"}'
```

## Legacy CTE-style (backward compatible)

The old CTE pattern still works:
```sql
WITH campaigns AS (
    SELECT * FROM ({{ flu_current_config() }})
    UNION ALL
    SELECT * FROM ({{ flu_previous_config() }})
)
```
