{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}

WITH demographics_with_age AS (
    SELECT
        dph.person_id,
        dph.effective_start_date,
        dph.effective_end_date,
        dph.birth_date_approx,
        dph.is_deceased,
        dph.ethnicity_category,
        dph.imd_quintile_19,
        dph.borough_registered as practice_borough,
        dph.practice_name,
        dph.practice_code,
        -- Generate months for each SCD-2 period
        ds.month_end_date as analysis_month,
        -- Calculate age for each month
        FLOOR(DATEDIFF(month, dph.birth_date_approx, ds.month_end_date) / 12) as age
    FROM {{ ref('dim_person_demographics_historical') }} dph
    CROSS JOIN {{ ref('int_date_spine') }} ds
    WHERE ds.month_end_date >= DATEADD('month', -48, CURRENT_DATE)
        AND ds.month_end_date <= LAST_DAY(CURRENT_DATE)
        -- Temporal join: month must fall within the SCD-2 period
        AND ds.month_end_date >= dph.effective_start_date
        AND (dph.effective_end_date IS NULL OR ds.month_end_date < dph.effective_end_date)
        -- Pre-filter to relevant ages to reduce processing
        AND dph.birth_date_approx IS NOT NULL
)

SELECT DISTINCT
        dwa.analysis_month,
        -- Fiscal year end flag
        CASE
        WHEN EXTRACT(MONTH FROM dwa.analysis_month) = 3
             AND EXTRACT(DAY FROM dwa.analysis_month) = 31
        THEN 1
        ELSE 0
        END AS is_fiscal_year_end,
        -- Fiscal year label
        CASE
        WHEN EXTRACT(MONTH FROM dwa.analysis_month) >= 4
        THEN TO_CHAR(dwa.analysis_month, 'YYYY') || '-' || RIGHT(TO_CHAR(EXTRACT(YEAR FROM dwa.analysis_month) + 1), 2)
        ELSE TO_CHAR(EXTRACT(YEAR FROM dwa.analysis_month) - 1) || '-' || RIGHT(TO_CHAR(EXTRACT(YEAR FROM dwa.analysis_month)), 2)
        END AS fiscal_year_label,
        dwa.person_id,
        dwa.age,
        dwa.is_deceased,
        dwa.birth_date_approx,
        -- Born after specific dates flags
        CASE WHEN dwa.birth_date_approx >= '2024-07-01' THEN 'Yes'
        ELSE 'No' END AS born_jul_2024_flag,
        CASE WHEN dwa.birth_date_approx >= '2025-01-01' THEN 'Yes'
        ELSE 'No' END AS born_jan_2025_flag,
        -- Birthday milestones
        DATEADD(YEAR, 1, dwa.birth_date_approx) as first_bday,
        DATEADD(YEAR, 12, dwa.birth_date_approx) as twelfth_bday,
        DATEADD(YEAR, 13, dwa.birth_date_approx) as thirteenth_bday,
        -- Ethnicity
        dwa.ethnicity_category,
        CASE
        WHEN dwa.ethnicity_category = 'Asian' THEN 1
        WHEN dwa.ethnicity_category = 'Black' THEN 2
        WHEN dwa.ethnicity_category = 'Mixed' THEN 3
        WHEN dwa.ethnicity_category = 'Other' THEN 4
        WHEN dwa.ethnicity_category = 'White' THEN 5
        WHEN dwa.ethnicity_category = 'Unknown' THEN 6
        END AS ethcat_order,
        -- IMD
        CASE WHEN dwa.imd_quintile_19 IS NULL THEN 'Unknown'
        ELSE dwa.imd_quintile_19 END AS imd_quintile,
        CASE
        WHEN dwa.imd_quintile_19 = 'Most Deprived' THEN 1
        WHEN dwa.imd_quintile_19 = 'Second Most Deprived' THEN 2
        WHEN dwa.imd_quintile_19 = 'Third Most Deprived' THEN 3
        WHEN dwa.imd_quintile_19 = 'Second Least Deprived' THEN 4
        WHEN dwa.imd_quintile_19 = 'Least Deprived' THEN 5
        ELSE 6 END AS imdquintile_order,
        -- Practice
        dwa.practice_borough,
        dwa.practice_name,
        dwa.practice_code
    FROM demographics_with_age dwa
    WHERE dwa.age IN (1, 2, 5, 11, 16)
        -- Temporarily exclude deceased patients (age frozen at death)
        AND dwa.is_deceased = FALSE
