{{
    config(
        materialized='table',
        cluster_by=['end_date'],
        tags=['monthly-full']
    )
}}

-- Month-level source coverage for lag-aware service activity.

WITH reference_months AS (
    SELECT DISTINCT month_end_date AS end_date
    FROM {{ ref('int_segmentation_person_month_spine') }}
),

community_source_months AS (
    SELECT
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MIN(CAST(start_date AS DATE)) AS first_date_in_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_csds_encounters') }}
    WHERE start_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))
),

community_source_bounds AS (
    SELECT MIN(first_date_in_month) AS source_first_date
    FROM community_source_months
),

community_reference_dates AS (
    SELECT
        r.end_date,
        MAX(c.last_date_in_month) AS community_window_end_date
    FROM reference_months AS r
    LEFT JOIN community_source_months AS c
        ON c.activity_month <= DATE_TRUNC('month', r.end_date)
    GROUP BY r.end_date
)

SELECT
    r.end_date,
    b.source_first_date AS community_source_first_date,
    r.community_window_end_date,
    DATEADD('month', -12, r.community_window_end_date)
        AS community_window_start_date,
    COALESCE(
        DATEADD('month', -12, r.community_window_end_date)
            >= b.source_first_date,
        FALSE
    ) AS is_community_window_complete,
    DATEDIFF('day', r.community_window_end_date, r.end_date)
        AS community_lag_days
FROM community_reference_dates AS r
CROSS JOIN community_source_bounds AS b
