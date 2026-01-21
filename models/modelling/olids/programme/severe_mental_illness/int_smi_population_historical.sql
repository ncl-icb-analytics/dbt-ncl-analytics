{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

--SMI REGISTER BASE POPULATION HISTORICAL (using HAS_SMI flag)

SELECT
    pmab.analysis_month,
    -- -- Fiscal year end flag
    -- CASE
    --     WHEN EXTRACT(MONTH FROM pmab.analysis_month) = 3
    --          AND EXTRACT(DAY FROM pmab.analysis_month) = 31
    --     THEN 1
    --     ELSE 0
    -- END AS is_fiscal_year_end,
    -- Fiscal year label (use existing field from person_month_analysis_base)
    pmab.financial_year as fiscal_year_label,
    pmab.person_id,
    pmab.age,
    pmab.is_deceased,
    pmab.birth_date_approx,
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
    COALESCE(pmab.imd_quintile_25, 'Unknown') AS imd_quintile,
        CASE
        WHEN pmab.imd_quintile_25 = 'Most Deprived' THEN 1
        WHEN pmab.imd_quintile_25 = 'Second Most Deprived' THEN 2
        WHEN pmab.imd_quintile_25 = 'Third Most Deprived' THEN 3
        WHEN pmab.imd_quintile_25 = 'Second Least Deprived' THEN 4
        WHEN pmab.imd_quintile_25 = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order,
    -- Practice
    pmab.borough_registered as practice_borough,
    COALESCE(pmab.neighbourhood_resident,'Unknown') as residential_neighbourhood,
    pmab.practice_name,
    pmab.practice_code,
    CASE
    WHEN pmab.LOCAL_AUTHORITY_NAME not in ('Barnet','Camden','Enfield','Haringey','Islington') 
    THEN 'Outside NCL' 
    WHEN pmab.LOCAL_AUTHORITY_NAME IS NULL THEN 'Unknown'
    ELSE pmab.LOCAL_AUTHORITY_NAME END as RESIDENTIAL_BOROUGH,
    CASE
    WHEN pmab.LOCAL_AUTHORITY_NAME not in ('Barnet','Camden','Enfield','Haringey','Islington') 
    THEN 'Outside NCL'
    WHEN pmab.LOCAL_AUTHORITY_NAME IS NULL THEN 'Unknown'
    ELSE pmab.ward_name END as WARD_NAME
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE  pmab
WHERE HAS_SMI = TRUE
    -- Exclude deceased patients (age frozen at death)
    AND pmab.is_deceased = FALSE
    -- Limit to last 48 months (4 years)
    AND pmab.analysis_month >= DATEADD('month', -48, CURRENT_DATE)
