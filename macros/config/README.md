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

## Campaign Sets

The COVID and flu models build every campaign listed in `covid_reported_campaign_ids()`
and `flu_reported_campaign_ids()`, emitted as one CTE by `covid_reported_campaigns()` and
`flu_reported_campaigns()`:

```sql
WITH all_campaigns AS (
    {{ covid_reported_campaigns() }}
)
```

Campaigns are added to those lists and never removed, so a season that has been reported
keeps its rows when the next one is added.

## Adding a Campaign Year

1. Add the campaign block to `flu_campaign_config()` or `covid_campaign_config()` with the
   dates from that year's UKHSA specification.
2. Append the campaign id to `flu_reported_campaign_ids()` or
   `covid_reported_campaign_ids()`.
3. Point the current and previous selectors at the new season:
   - `flu_campaign_selection.sql` → `flu_current_campaign()`, `flu_previous_campaign()`
   - `covid_campaign_selection.sql` → `covid_current_autumn()`, etc.
4. Add the campaign to the label and sort expressions in the dashboard models and to the
   `campaign_year` mapping in `fct_covid_flu_uptake`.

QOF has its own reference date in `qof_config.sql` → `qof_reference_date()`.
