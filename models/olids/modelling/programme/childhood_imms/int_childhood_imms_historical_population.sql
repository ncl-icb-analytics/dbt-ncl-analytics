{{
    config(
        materialized='table',
        tags=['childhood_imms'])
        
}}

SELECT DISTINCT
        dph.analysis_month,
        --flag to determine which analysis month is fiscal year end
        CASE 
        WHEN EXTRACT(MONTH FROM dph.analysis_month) = 3 
             AND EXTRACT(DAY FROM dph.analysis_month) = 31
        THEN 1
        ELSE 0
        END AS is_fiscal_year_end,
        CASE 
        WHEN EXTRACT(MONTH FROM dph.analysis_month) >= 4 
        THEN TO_CHAR(dph.analysis_month, 'YYYY') || '-' || RIGHT(TO_CHAR(EXTRACT(YEAR FROM dph.analysis_month) + 1), 2)
        ELSE TO_CHAR(EXTRACT(YEAR FROM dph.analysis_month) - 1) || '-' || RIGHT(TO_CHAR(EXTRACT(YEAR FROM dph.analysis_month)), 2)
        END AS fiscal_year_label,
        dph.person_id,
        dph.age,
        dph.birth_date_approx,
        DATEADD(YEAR,1,dph.BIRTH_DATE_APPROX) as FIRST_BDAY,
        DATEADD(YEAR,12,dph.BIRTH_DATE_APPROX) as TWELFTH_BDAY,
        DATEADD(YEAR,13,dph.BIRTH_DATE_APPROX) as THIRTEENTH_BDAY,
        dph.ethnicity_category,
        CASE 
        WHEN dph.ETHNICITY_CATEGORY = 'Asian' THEN 1
        WHEN dph.ETHNICITY_CATEGORY = 'Black' THEN 2
        WHEN dph.ETHNICITY_CATEGORY = 'Mixed' THEN 3
        WHEN dph.ETHNICITY_CATEGORY = 'Other' THEN 4
        WHEN dph.ETHNICITY_CATEGORY = 'White' THEN 5
        WHEN dph.ETHNICITY_CATEGORY = 'Unknown' THEN 6
        END AS ETHCAT_ORDER,
        dph.imd_quintile_19 as IMD_QUINTILE,
        dph.BOROUGH_REGISTERED AS PRACTICE_BOROUGH,
         dph.practice_name,
        dph.practice_code
    FROM {{ ref('dim_person_demographics_historical') }} dph
    WHERE dph.analysis_month >= DATEADD('month', -60, CURRENT_DATE)
        AND dph.analysis_month <= LAST_DAY(CURRENT_DATE)
        AND dph.age in (1,2,5,11,16)
        AND ICB_CODE = 'QMJ'