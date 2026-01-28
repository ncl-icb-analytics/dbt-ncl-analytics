{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--This table creates dose count and date labels for childhood immunisations for children aged under 11 years old using a base table
-- Creating CTE for 6in1 DOSE 1
WITH SIXIN1_DOSE1 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS sixin1_dose1_sort,
        v.FISCAL_YEAR as sixin1_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose1_label,
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_1'  AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 2
,SIXIN1_DOSE2 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose2_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS sixin1_dose2_sort,
       v.FISCAL_YEAR as sixin1_dose2_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_2'  AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for 6in1 DOSE 3
,SIXIN1_DOSE3 AS (
       SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS sixin1_dose3_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS sixin1_dose3_sort,
       v.FISCAL_YEAR as sixin1_dose3_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose3_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_3'  AND v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for Rotavirus DOSE 1
,ROTA_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS rota_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS rota_dose1_sort,
        v.FISCAL_YEAR as rota_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS rota_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'ROTA_1' AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for Rotavirus DOSE 2
,ROTA_DOSE2 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS rota_dose2_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS rota_dose2_sort,
        v.FISCAL_YEAR as rota_dose2_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS rota_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'ROTA_2' AND v.EVENT_TYPE = 'Administration'
)  
 -- Creating CTE for MenB DOSE 1
,MENB_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS menb_dose1_sort,
        v.FISCAL_YEAR as menb_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_1' and v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for MenB DOSE 1
,MENB_DOSE2 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose2_date, 
       TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS menb_dose2_sort,
        v.FISCAL_YEAR as menb_dose2_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_2' AND v.EVENT_TYPE = 'Administration'
)
 -- Creating CTE for MenB DOSE 1
,MENB_DOSE3 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS menb_dose3_date, 
       TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS menb_dose3_sort,
        v.FISCAL_YEAR as menb_dose3_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose3_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_3' AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for PCV DOSE 1
,PCV_DOSE1 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS pcv_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS pcv_dose1_sort,
        v.FISCAL_YEAR as pcv_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PCV_1' AND v.EVENT_TYPE = 'Administration'
         )
-- Creating CTE for PCV DOSE 2
,PCV_DOSE2 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS pcv_dose2_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS pcv_dose2_sort,
        v.FISCAL_YEAR as pcv_dose2_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PCV_2' AND v.EVENT_TYPE = 'Administration'
         )
 -- Creating CTE for HIBMENC DOSE 1
,HIBMENC_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS hibmc_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS hibmc_dose1_sort,
        v.FISCAL_YEAR as hibmc_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS hibmc_dose1_label
       FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'HIBMENC_1' AND v.EVENT_TYPE = 'Administration'
)
-- Creating CTE for MMR Dose 1
,MMR_DOSE1 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS mmr_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS  mmr_dose1_sort,
        v.FISCAL_YEAR as mmr_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmr_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MMR_1' AND v.EVENT_TYPE = 'Administration'
)  
-- Creating CTE for MMR DOSE 2
,MMR_DOSE2 AS (
    SELECT 
       v.PERSON_ID,
        v.EVENT_DATE AS mmr_dose2_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS mmr_dose2_sort,
        v.FISCAL_YEAR as mmr_dose2_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS mmr_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MMR_2' AND v.EVENT_TYPE = 'Administration'
)

-- Creating CTE for FOURIN1 DOSE 1
,FOURIN1_DOSE1 AS (
    SELECT 
        v.PERSON_ID,
        v.EVENT_DATE AS fourin1_dose1_date, 
        TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS fourin1_dose1_sort,
        v.FISCAL_YEAR as fourin1_dose1_fiscal,
        MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS fourin1_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '4IN1_1' AND v.EVENT_TYPE = 'Administration'
         )
        

SELECT DISTINCT
v.PERSON_ID 
,v.GP_NAME
,v.practice_code
,sixin1_dose1_label
,sixin1_dose1_fiscal
,sixin1_dose1_sort
,sixin1_dose2_label
,sixin1_dose2_fiscal
,sixin1_dose2_sort
,sixin1_dose3_label
,sixin1_dose3_fiscal
,sixin1_dose3_sort
,rota_dose1_label
,rota_dose1_fiscal
,rota_dose1_sort
,rota_dose2_label
,rota_dose2_fiscal
,rota_dose2_sort
,menb_dose1_label
,menb_dose1_fiscal
,menb_dose1_sort
,menb_dose2_label
,menb_dose2_fiscal
,menb_dose2_sort
,menb_dose3_label
,menb_dose3_fiscal
,menb_dose3_sort
,pcv_dose1_label
,pcv_dose1_fiscal
,pcv_dose1_sort
,pcv_dose2_label
,pcv_dose2_fiscal
,pcv_dose2_sort
,hibmc_dose1_label
,hibmc_dose1_fiscal
,hibmc_dose1_sort
,mmr_dose1_label
,mmr_dose1_fiscal
,mmr_dose1_sort
,mmr_dose2_label
,mmr_dose2_fiscal
,mmr_dose2_sort
,fourin1_dose1_label
,fourin1_dose1_fiscal
,fourin1_dose1_sort
FROM {{ ref('int_childhood_imms_dose_base_child') }} v
left join SIXIN1_DOSE1 s1 using (PERSON_ID)
left join SIXIN1_DOSE2 s2 using (PERSON_ID)
left join SIXIN1_DOSE3 s3 using (PERSON_ID)
left join ROTA_DOSE1 r1 using (PERSON_ID)
left join ROTA_DOSE2 r2 using (PERSON_ID)
left join MENB_DOSE1 m1 using (PERSON_ID)
left join MENB_DOSE2 m2 using (PERSON_ID)
left join MENB_DOSE3 m3 using (PERSON_ID)
left join PCV_DOSE1 p1 using (PERSON_ID) 
left join PCV_DOSE2 p2 using (PERSON_ID) 
left join HIBMENC_DOSE1 h using (PERSON_ID)
left join MMR_DOSE1 mr1 using (PERSON_ID)
left join MMR_DOSE2 mr2 using (PERSON_ID)
left join FOURIN1_DOSE1 f using (PERSON_ID)
