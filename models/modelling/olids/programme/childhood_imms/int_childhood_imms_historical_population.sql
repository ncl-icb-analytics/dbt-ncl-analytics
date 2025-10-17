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
        dph.is_deceased,
        dph.birth_date_approx,
        CASE WHEN dph.BIRTH_DATE_APPROX >= '2024-07-01' THEN 'Yes'
        ELSE 'No' END AS BORN_JUL_2024_FLAG,
        CASE WHEN dph.BIRTH_DATE_APPROX >= '2025-01-01' THEN 'Yes'
        ELSE 'No' END AS BORN_JAN_2025_FLAG,
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
        CASE WHEN dph.IMD_QUINTILE_19 IS NULL THEN 'Unknown' 
        ELSE dph.IMD_QUINTILE_19 END AS IMD_QUINTILE,
        CASE 
        WHEN dph.IMD_QUINTILE_19 = 'Most Deprived' THEN 1
        WHEN dph.IMD_QUINTILE_19 = 'Second Most Deprived' THEN 2
        WHEN dph.IMD_QUINTILE_19 = 'Third Most Deprived' THEN 3
        WHEN dph.IMD_QUINTILE_19 = 'Second Least Deprived' THEN 4
        WHEN dph.IMD_QUINTILE_19 = 'Least Deprived' THEN 5
        ELSE 6 END AS IMDQUINTILE_ORDER,
        dph.BOROUGH_REGISTERED AS PRACTICE_BOROUGH,
         dph.practice_name,
        dph.practice_code
    FROM {{ ref('dim_person_demographics_historical') }} dph
    WHERE dph.analysis_month >= DATEADD('month', -48, CURRENT_DATE)
        AND dph.analysis_month <= LAST_DAY(CURRENT_DATE)
        AND dph.age in (1,2,5,11,16)
        -- AND ICB_CODE = 'QMJ'
        --temporarily exclude deceased patients because their age is frozen at death and appearing incorrectly in the denominator in more recent months
        AND dph.is_deceased = FALSE