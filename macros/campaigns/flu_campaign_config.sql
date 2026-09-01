/*
Flu Campaign Configuration - Single Source of Truth

This macro provides all campaign-specific dates and parameters in one place.
Instead of scattered hardcoded dates, everything is defined here clearly.

MULTI-CAMPAIGN SUPPORT:
All flu models work with any campaign year by changing the flu_current_campaign variable.

Available campaigns:
- 'Flu 2023-24' - 2023-24 Flu Vaccination Campaign
- 'Flu 2024-25' - 2024-25 Flu Vaccination Campaign
- 'Flu 2025-26' - 2025-26 Flu Vaccination Campaign
- 'Flu 2026-27' - 2026-27 Flu Vaccination Campaign (default)

The campaigns the models actually build are listed in flu_reported_campaigns()
in macros/config/flu_campaign_selection.sql.

Usage Examples:
- Default campaign: {{ flu_campaign_config() }}
- Specific campaign: {{ flu_campaign_config('Flu 2023-24') }}
- Via dbt_project.yml: Set flu_current_campaign variable, then run normally

Configuration in dbt_project.yml:
vars:
  flu_current_campaign: "Flu 2026-27"     # Change this to switch campaigns
  flu_previous_campaign: "Flu 2025-26"    # For comparison queries

SOURCE:
UKHSA Seasonal Influenza Vaccine Uptake Reporting Specification, PRIMIS v16.0,
23 July 2026 (titled 2526 but published for the 2026-27 collection year).

AUDIT END DATE:
audit_end_date is the spec AUDITEND_DAT. It rolls forward with CURRENT_DATE while a
campaign is running and stops at that campaign's campaign_end_date, so closed seasons
do not keep absorbing newly recorded events.

IMMUNOSUPPRESSION ADMIN CODES:
Spec v16.0 widened IMMADM_DAT from a fixed six-month floor to any code in the three
years before AUDITEND_DAT. immuno_admin_lookback_date carries that per campaign, so
closed seasons keep the six-month rule they were reported under.

CHILD AGE GROUPS:
- Preschool: ages 2-3 at CHILD_DAT (31 August in the campaign year)
- School age: ages 4-15 at CHILD_DAT
- Age-agnostic model names allow for consistent year-to-year comparisons
*/

{% macro flu_campaign_config(campaign_id='Flu 2026-27') %}
    {%- if campaign_id == 'Flu 2023-24' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2023-24 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates
            '2023-09-01'::DATE AS campaign_start_date,
            '2024-03-31'::DATE AS campaign_reference_date,
            '2023-08-31'::DATE AS child_reference_date,
            '2024-03-31'::DATE AS campaign_end_date,

            -- Medication lookback dates
            '2022-09-01'::DATE AS asthma_medication_lookback_date,
            '2023-03-01'::DATE AS immuno_medication_lookback_date,
            '2023-03-01'::DATE AS immuno_admin_lookback_date,

            -- Child age group birth date ranges (campaign-specific)
            '2019-09-01'::DATE AS child_preschool_birth_start,
            '2021-08-31'::DATE AS child_preschool_birth_end,
            '2007-09-01'::DATE AS child_school_age_birth_start,
            '2019-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates
            '2023-08-31'::DATE AS flu_vaccination_after_date,
            '2023-08-31'::DATE AS laiv_vaccination_after_date,

            -- Long-stay residential care was a flu indicator up to spec v15.6
            TRUE AS eligible_long_term_residential_care,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            LEAST(CURRENT_DATE, '2024-03-31'::DATE) AS audit_end_date
    {%- elif campaign_id == 'Flu 2024-25' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2024-25 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates
            '2024-09-01'::DATE AS campaign_start_date,
            '2025-03-31'::DATE AS campaign_reference_date,
            '2024-08-31'::DATE AS child_reference_date,
            '2025-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates
            '2023-09-01'::DATE AS asthma_medication_lookback_date,
            '2024-03-01'::DATE AS immuno_medication_lookback_date,
            '2024-03-01'::DATE AS immuno_admin_lookback_date,

            -- Child age group birth date ranges (campaign-specific)
            '2020-09-01'::DATE AS child_preschool_birth_start,
            '2022-08-31'::DATE AS child_preschool_birth_end,
            '2008-09-01'::DATE AS child_school_age_birth_start,
            '2020-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates
            '2024-08-31'::DATE AS flu_vaccination_after_date,
            '2024-08-31'::DATE AS laiv_vaccination_after_date,

            -- Long-stay residential care was a flu indicator up to spec v15.6
            TRUE AS eligible_long_term_residential_care,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            LEAST(CURRENT_DATE, '2025-02-28'::DATE) AS audit_end_date
    {%- elif campaign_id == 'Flu 2025-26' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2025-26 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates (shifted +1 year)
            '2025-09-01'::DATE AS campaign_start_date,
            '2026-03-31'::DATE AS campaign_reference_date,
            '2025-08-31'::DATE AS child_reference_date,
            '2026-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates (shifted +1 year)
            '2024-09-01'::DATE AS asthma_medication_lookback_date,
            '2025-03-01'::DATE AS immuno_medication_lookback_date,
            '2025-03-01'::DATE AS immuno_admin_lookback_date,

            -- Child age group birth date ranges (campaign-specific)
            '2021-09-01'::DATE AS child_preschool_birth_start,
            '2023-08-31'::DATE AS child_preschool_birth_end,
            '2009-09-01'::DATE AS child_school_age_birth_start,
            '2021-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates (shifted +1 year)
            '2025-08-31'::DATE AS flu_vaccination_after_date,
            '2025-08-31'::DATE AS laiv_vaccination_after_date,

            -- Spec v15.7 removed the long-stay residential care indicator. The cohort is
            -- kept on for 2025-26 because the season was reported with it.
            TRUE AS eligible_long_term_residential_care,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            LEAST(CURRENT_DATE, '2026-02-28'::DATE) AS audit_end_date
    {%- elif campaign_id == 'Flu 2026-27' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2026-27 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates (spec 2.1: START_DAT 01/09/26, REF_DAT 31/03/27,
            -- CHILD_DAT 31/08/26, final AUDITEND_DAT 28/02/27)
            '2026-09-01'::DATE AS campaign_start_date,
            '2027-03-31'::DATE AS campaign_reference_date,
            '2026-08-31'::DATE AS child_reference_date,
            '2027-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates (spec 2.3)
            '2025-09-01'::DATE AS asthma_medication_lookback_date,      -- ASTMED_DAT and ASTRX_DAT from 01/09/25
            '2026-03-01'::DATE AS immuno_medication_lookback_date,      -- IMMRX_DAT and DXT_CHEMO_DAT from 01/03/26
            DATEADD('year', -3, LEAST(CURRENT_DATE, '2027-02-28'::DATE)) AS immuno_admin_lookback_date,
                                                                        -- IMMADM_DAT: 3 years before AUDITEND_DAT

            -- Child age group birth date ranges: ages 2-3 and 4-15 at CHILD_DAT (spec 4.1)
            '2022-09-01'::DATE AS child_preschool_birth_start,
            '2024-08-31'::DATE AS child_preschool_birth_end,
            '2010-09-01'::DATE AS child_school_age_birth_start,
            '2022-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates (spec: vaccination fields are after 31/08/26)
            '2026-08-31'::DATE AS flu_vaccination_after_date,
            '2026-08-31'::DATE AS laiv_vaccination_after_date,

            -- Spec v15.7 removed the long-stay residential care indicator from the flu
            -- programme, so the cohort is not reported from 2026-27.
            FALSE AS eligible_long_term_residential_care,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            LEAST(CURRENT_DATE, '2027-02-28'::DATE) AS audit_end_date
    {%- else -%}
        -- Default to current campaign if unknown campaign_id
        {{ flu_campaign_config('Flu 2026-27') }}
    {%- endif -%}
{% endmacro %}

/*
Helper macro to get a specific campaign date
Usage: {{ flu_get_campaign_date('campaign_reference_date') }}
*/
{% macro flu_get_campaign_date(date_name, campaign_id=none) %}
    {%- set campaign_id = campaign_id or var('flu_current_campaign', 'Flu 2026-27') -%}
    (SELECT {{ date_name }} FROM ({{ flu_campaign_config(campaign_id) }}))
{% endmacro %}
