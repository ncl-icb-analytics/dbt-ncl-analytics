{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the MMRV dose 2 vaccination events for this population - LIMIT to 1st JAN 2026 official introduction date
  SELECT DISTINCT
        v.PERSON_ID
        ,v.practice_code
          ,v.EVENT_DATE AS mmrv_dose2_date 
        ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS  mmrv_dose2_sort
        ,v.FISCAL_YEAR as mmrv_dose2_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS  mmrv_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
         --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
        --restrict to administered doses only
        WHERE v.VACCINE_ID in ('MMRV_2','MMRV_2B') AND v.EVENT_TYPE = 'Administration'
        AND v.EVENT_DATE >= '2026-01-01'