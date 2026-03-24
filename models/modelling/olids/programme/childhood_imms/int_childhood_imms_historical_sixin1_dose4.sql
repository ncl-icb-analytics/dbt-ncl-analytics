{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the sixin1 dose4 vaccination events for this population. - LIMIT to 1st JAN 2026 official introduction date
SELECT 
        v.PERSON_ID
        ,v.age
        ,v.practice_code
        ,v.EVENT_DATE AS sixin1_dose4_date
        ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS sixin1_dose4_sort
       ,v.FISCAL_YEAR as sixin1_dose4_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS sixin1_dose4_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
        --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '6IN1_4'  AND v.EVENT_TYPE LIKE 'Admin%'
       AND v.EVENT_DATE >= '2026-01-01'