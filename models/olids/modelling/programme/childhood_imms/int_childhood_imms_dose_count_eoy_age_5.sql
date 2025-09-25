{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH FISCAL5YRBASE AS (
select distinct
p.PERSON_ID
,p.analysis_month
,p.fiscal_year_label
,p.practice_name as GP_NAME
,p.practice_code
-- ,p.age
-- ,p.ethnicity_category
-- ,p.ethcat_order
-- ,p.imd_quintile
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
FROM {{ ref('int_childhood_imms_historical_population') }} p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--restrict by AGE not by VACCINE as for currently aged 5- otherwise base population is not correct
WHERE p.age = 5 AND is_fiscal_year_end = 1


)
-- Creating CTE for 6in1 DOSE 1
,SIXIN1_DOSE1 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose1_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ORDER = 1  AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 2
,SIXIN1_DOSE2 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose2_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose2_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose2_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose2_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ORDER = 4  AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 3
,SIXIN1_DOSE3 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose3_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose3_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose3_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose3_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ORDER = 7  AND v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for HIBMENC DOSE 1
,HIBMENC_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS hibmc_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS hibmc_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS hibmc_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS hibmc_dose1_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ORDER = 9 AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for MMR Dose 1
,MMR_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS mmr_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS  mmr_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmr_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmr_dose1_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ORDER = 11 AND v.EVENT_TYPE = 'Administration'
)  
 -- Creating CTE for MMR DOSE 2
,MMR_DOSE2 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS mmr_dose2_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS mmr_dose2_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS mmr_dose2_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS mmr_dose2_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ORDER = 15 AND v.EVENT_TYPE = 'Administration'
)

-- Creating CTE for FOURIN1 DOSE 1
,FOURIN1_DOSE1 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS fourin1_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS fourin1_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS fourin1_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS fourin1_dose1_month_year_label
        FROM FISCAL5YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ORDER = 14 AND v.EVENT_TYPE = 'Administration'
         )
       
--COMBINED
SELECT DISTINCT 
v.PERSON_ID 
,v.ANALYSIS_MONTH
,v.fiscal_year_label AS FISCAL_YEAR
,v.GP_NAME
,v.practice_code
,sixin1_dose1_month_year_label
,sixin1_dose1_sort
,sixin1_dose2_month_year_label
,sixin1_dose2_sort
,sixin1_dose3_month_year_label
,sixin1_dose3_sort
,fourin1_dose1_month_year_label
,fourin1_dose1_sort
,hibmc_dose1_month_year_label
,hibmc_dose1_sort
,mmr_dose1_month_year_label
,mmr_dose1_sort
,mmr_dose2_month_year_label
,mmr_dose2_sort
FROM FISCAL5YRBASE v 
left join SIXIN1_DOSE1 s1 using (PERSON_ID)
left join SIXIN1_DOSE2 s2 using (PERSON_ID)
left join SIXIN1_DOSE3 s3 using (PERSON_ID)
left join HIBMENC_DOSE1 h1 using (PERSON_ID)
left join MMR_DOSE1 mr1 using (PERSON_ID)
left join MMR_DOSE2 mr2 using (PERSON_ID)
left join FOURIN1_DOSE1 f1 using (PERSON_ID) 