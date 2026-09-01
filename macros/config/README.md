# Configuration Macros

Centralised configuration for campaigns, QOF, and observability settings.

## Campaign sets

The COVID and flu models build every campaign in `covid_reported_campaign_ids()` /
`flu_reported_campaign_ids()`:

```sql
WITH all_campaigns AS (
    {{ covid_reported_campaigns() }}
)
```

Campaigns are added to those lists and never removed, so a season that has been reported
keeps its rows when the next one is added.

For a single campaign's parameters, use `covid_autumn_config()`, `flu_current_config()`
and their siblings, or call `covid_campaign_config('COVID Autumn 2026')` directly. Dates
live in `macros/campaigns/*_campaign_config.sql` only.

## QOF

```sql
{{ qof_reference_date() }}

-- QOF register
WHERE clinical_effective_date <= {{ qof_reference_date() }}
```

## Adding a campaign year

1. Add the campaign block to `flu_campaign_config()` or `covid_campaign_config()` with the
   dates from that year's UKHSA specification.
2. Append the campaign id to `flu_reported_campaign_ids()` or
   `covid_reported_campaign_ids()`.
3. Point the season-in-flight selectors at the new season. These name the current season
   for models that need it; they do not decide which campaigns get built:
   - `flu_campaign_selection.sql` → `flu_current_campaign()`, `flu_previous_campaign()`
   - `covid_campaign_selection.sql` → `covid_current_autumn()`, etc.
4. Add the campaign to the label and sort expressions in `int_covid_flu_dashboard_current`,
   `int_covid_flu_dashboard_wide` and `covid_flu_dashboard_base`, to the `campaign_year`
   mapping in `fct_covid_flu_uptake`, and to the `campaign_id` comment in
   `sem_olids_vaccinations`.

QOF has its own reference date in `qof_config.sql` → `qof_reference_date()`.
