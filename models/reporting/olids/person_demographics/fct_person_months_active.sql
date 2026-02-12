{{
    config(
        materialized='table',
        cluster_by=['analysis_month', 'person_id'])
}}

-- Active Person-Months Spine
-- Pre-computes monthly active patient populations to eliminate repetitive spine logic
-- Analysts can join this to any temporal data without recreating the monthly spine
-- Uses effective_end_date (accounts for death and deregistration) rather than
-- point-in-time registration_status to correctly include historically-active patients

WITH monthly_spine AS (
    -- Create monthly date spine for the last 5 years
    SELECT
        DATE_TRUNC('month', dateadd('month', seq4() - 60, CURRENT_DATE)) as analysis_month
    FROM table(generator(rowcount => 60))  -- 5 years of monthly data
    WHERE analysis_month <= CURRENT_DATE
)

SELECT
    ms.analysis_month,
    hr.person_id,
    -- Include current practice for convenience (most recent if multiple)
    FIRST_VALUE(hr.practice_id) OVER (
        PARTITION BY ms.analysis_month, hr.person_id
        ORDER BY hr.is_current_registration DESC, hr.registration_start_date DESC
        ROWS UNBOUNDED PRECEDING
    ) as current_practice_id,
    FIRST_VALUE(hr.practice_name) OVER (
        PARTITION BY ms.analysis_month, hr.person_id
        ORDER BY hr.is_current_registration DESC, hr.registration_start_date DESC
        ROWS UNBOUNDED PRECEDING
    ) as current_practice_name

FROM monthly_spine ms
INNER JOIN {{ ref('dim_person_historical_practice') }} hr
    ON hr.registration_start_date <= LAST_DAY(ms.analysis_month)
    AND (hr.effective_end_date IS NULL OR hr.effective_end_date >= ms.analysis_month)

-- Ensure exactly one row per person-month
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ms.analysis_month, hr.person_id
    ORDER BY hr.is_current_registration DESC, hr.registration_start_date DESC
) = 1

ORDER BY analysis_month, person_id