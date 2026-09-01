/*
Campaign search population, as at the campaign rather than as at today.

Both specs require the patient to be currently registered for GMS at RUN_DAT. Testing
that against dim_person_demographics answers a different question - is this person
registered NOW - which silently rewrites closed campaigns: anyone who has since left the
list or died drops out of a season that was already reported.

This resolves each campaign to a monthly population snapshot and returns the people who
were registered and alive at that point. A campaign whose reference date is still in the
future, which is any season in flight, takes the most recent snapshot available.
*/

{{ config(
    materialized='table',
    tags=['covid_flu'],
    cluster_by=['campaign_id', 'person_id']
) }}

WITH campaigns AS (
    SELECT campaign_id, campaign_reference_date
    FROM ({{ covid_reported_campaigns() }})

    UNION ALL

    SELECT campaign_id, campaign_reference_date
    FROM ({{ flu_reported_campaigns() }})
),

-- The newest snapshot the warehouse holds. A season in flight has no snapshot at its own
-- reference date, so it falls back to this.
latest_snapshot AS (
    SELECT MAX(analysis_month) AS latest_month
    FROM {{ ref('person_month_analysis_base') }}
),

campaign_month AS (
    SELECT
        c.campaign_id,
        c.campaign_reference_date,
        LAST_DAY(LEAST(c.campaign_reference_date, l.latest_month)) AS population_month
    FROM campaigns c
    CROSS JOIN latest_snapshot l
)

SELECT
    cm.campaign_id,
    cm.campaign_reference_date,
    cm.population_month,
    pmab.person_id
FROM campaign_month cm
JOIN {{ ref('person_month_analysis_base') }} pmab
    ON pmab.analysis_month = cm.population_month
WHERE pmab.is_active = TRUE
    AND COALESCE(pmab.is_deceased, FALSE) = FALSE
