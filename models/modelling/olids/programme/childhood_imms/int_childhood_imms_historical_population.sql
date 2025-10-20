{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}

SELECT
    pmab.analysis_month,
    -- Fiscal year end flag
    CASE
        WHEN EXTRACT(MONTH FROM pmab.analysis_month) = 3
             AND EXTRACT(DAY FROM pmab.analysis_month) = 31
        THEN 1
        ELSE 0
    END AS is_fiscal_year_end,
    -- Fiscal year label (use existing field from person_month_analysis_base)
    pmab.financial_year as fiscal_year_label,
    pmab.person_id,
    pmab.age,
    pmab.is_deceased,
    pmab.birth_date_approx,
    -- Born after specific dates flags
    CASE WHEN pmab.birth_date_approx >= '2024-07-01' THEN 'Yes'
        ELSE 'No' END AS born_jul_2024_flag,
    CASE WHEN pmab.birth_date_approx >= '2025-01-01' THEN 'Yes'
        ELSE 'No' END AS born_jan_2025_flag,
    -- Birthday milestones
    DATEADD(YEAR, 1, pmab.birth_date_approx) as first_bday,
    DATEADD(YEAR, 12, pmab.birth_date_approx) as twelfth_bday,
    DATEADD(YEAR, 13, pmab.birth_date_approx) as thirteenth_bday,
    -- Ethnicity
    pmab.ethnicity_category,
    CASE
        WHEN pmab.ethnicity_category = 'Asian' THEN 1
        WHEN pmab.ethnicity_category = 'Black' THEN 2
        WHEN pmab.ethnicity_category = 'Mixed' THEN 3
        WHEN pmab.ethnicity_category = 'Other' THEN 4
        WHEN pmab.ethnicity_category = 'White' THEN 5
        WHEN pmab.ethnicity_category = 'Unknown' THEN 6
    END AS ethcat_order,
    -- IMD
    CASE WHEN pmab.imd_quintile_19 IS NULL THEN 'Unknown'
        ELSE pmab.imd_quintile_19 END AS imd_quintile,
    CASE
        WHEN pmab.imd_quintile_19 = 'Most Deprived' THEN 1
        WHEN pmab.imd_quintile_19 = 'Second Most Deprived' THEN 2
        WHEN pmab.imd_quintile_19 = 'Third Most Deprived' THEN 3
        WHEN pmab.imd_quintile_19 = 'Second Least Deprived' THEN 4
        WHEN pmab.imd_quintile_19 = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order,
    -- Practice
    pmab.borough_registered as practice_borough,
    pmab.practice_name,
    pmab.practice_code
FROM {{ ref('person_month_analysis_base') }} pmab
WHERE pmab.age IN (1, 2, 5, 11, 16)
    -- Exclude deceased patients (age frozen at death)
    AND pmab.is_deceased = FALSE
    -- Limit to last 48 months (4 years)
    AND pmab.analysis_month >= DATEADD('month', -48, CURRENT_DATE)
