{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH FISCAL2YRBASE AS (
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
--restrict by AGE not by VACCINE as for currently aged 2 - otherwise base population is not correct
WHERE p.age = 2 AND is_fiscal_year_end = 1

)
-- Creating CTE for 6in1 DOSE 1
,SIXIN1_DOSE1 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose1_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_1' AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 2
,SIXIN1_DOSE2 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose2_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose2_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose2_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose2_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_2'  AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 3
,SIXIN1_DOSE3 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose3_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS sixin1_dose3_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose3_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose3_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_3'  AND v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for HIBMENC DOSE 1
,HIBMENC_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS hibmc_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS hibmc_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS hibmc_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS hibmc_dose1_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'HIBMENC_1' AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for MMR Dose 1
,MMR_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS mmr_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS  mmr_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmr_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmr_dose1_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MMR_1' AND v.EVENT_TYPE = 'Administration'
)  
 -- Creating CTE for MenB DOSE 1
,MENB_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS menb_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose1_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_1' and v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for MenB DOSE 2
,MENB_DOSE2 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose2_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS menb_dose2_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose2_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose2_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_2' AND v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for MenB DOSE 3
,MENB_DOSE3 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose3_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS menb_dose3_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose3_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose3_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_3' AND v.EVENT_TYPE = 'Administration'
    )
-- Creating CTE for PCV DOSE 1
,PCV_DOSE1 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS pcv_dose1_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS pcv_dose1_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose1_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose1_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PCV_1' AND v.EVENT_TYPE = 'Administration'
         )
-- Creating CTE for PCV DOSE 2
,PCV_DOSE2 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS pcv_dose2_date, 
        YEAR(v.EVENT_DATE) || '-' || MONTH(v.EVENT_DATE) AS pcv_dose2_sort,
        MONTH(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose2_month_year,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose2_month_year_label
        FROM FISCAL2YRBASE v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PCV_2' AND v.EVENT_TYPE = 'Administration'
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
,hibmc_dose1_month_year_label
,hibmc_dose1_sort
,mmr_dose1_month_year_label
,mmr_dose1_sort
,menb_dose1_month_year_label
,menb_dose1_sort
,menb_dose2_month_year_label
,menb_dose2_sort
,menb_dose3_month_year_label
,menb_dose3_sort
,pcv_dose1_month_year_label
,pcv_dose1_sort
,pcv_dose2_month_year_label
,pcv_dose2_sort
FROM FISCAL2YRBASE v 
left join SIXIN1_DOSE1 s1 using (PERSON_ID)
left join SIXIN1_DOSE2 s2 using (PERSON_ID)
left join SIXIN1_DOSE3 s3 using (PERSON_ID)
left join HIBMENC_DOSE1 h1 using (PERSON_ID)
left join MMR_DOSE1 mr1 using (PERSON_ID)
left join MENB_DOSE1 m1 using (PERSON_ID)
left join MENB_DOSE2 m2 using (PERSON_ID)
left join MENB_DOSE3 m3 using (PERSON_ID)
left join PCV_DOSE1 p1 using (PERSON_ID) 
left join PCV_DOSE2 p2 using (PERSON_ID)