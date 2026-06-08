{{
    config(
        materialized='table',
        tags=['utility', 'refresh_date', 'daily']
    )
}}

/*
Global OLIDS Data Refresh Date

Calculates the consensus date where at least 150 practices have uploaded data,
excluding future dates. This filters out both stale practices and those entering
incorrect future dates.

Returns a single row with the most recent valid data refresh date.
*/

WITH practice_list AS (
    SELECT DISTINCT practice_code
    FROM {{ ref('dim_practice') }}
),
date_practice_counts AS (
    SELECT
        o.clinical_effective_date::date AS obs_date,
        COUNT(DISTINCT o.publisher_organisation_code) AS practice_count
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN practice_list p
        ON o.publisher_organisation_code = p.practice_code
    WHERE o.clinical_effective_date < CURRENT_DATE()
        -- Most-recent valid-data date is always within recent weeks; constrain
        -- the scan to the last 3 months so the clinical_effective_date cluster
        -- key prunes the OBSERVATION scan (was a near-full ~1.6B row scan).
        AND o.clinical_effective_date >= DATEADD('month', -3, CURRENT_DATE())
    GROUP BY o.clinical_effective_date::date
)

SELECT
    obs_date AS global_data_refresh_date
FROM date_practice_counts
WHERE practice_count >= 150
ORDER BY obs_date DESC
LIMIT 1
