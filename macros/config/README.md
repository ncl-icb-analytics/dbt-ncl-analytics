# Configuration Macros

Centralised configuration for campaigns, QOF, and observability settings.

## Quick Reference

### Flu Campaign
```sql
{{ flu_current_campaign_start_date() }}
{{ flu_current_campaign_end_date() }}
{{ flu_current_campaign_reference_date() }}
{{ flu_previous_campaign_start_date() }}
-- etc.
```

### COVID Campaign
```sql
{{ covid_autumn_campaign_start_date() }}
{{ covid_autumn_campaign_end_date() }}
{{ covid_spring_campaign_start_date() }}
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

## Changing Campaign Years

Edit the campaign selector macros in the relevant file:

- `flu_campaign_selection.sql` → `flu_current_campaign()`, `flu_previous_campaign()`
- `covid_campaign_selection.sql` → `covid_current_autumn()`, etc.
- `qof_config.sql` → `qof_reference_date()`
