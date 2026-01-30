{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the FOUR IN 1 dose 1 vaccination events for this population
  SELECT 
        v.PERSON_ID
        ,v.practice_code
        ,v.EVENT_DATE AS fourin1_dose1_date 
        ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS fourin1_dose1_sort
        ,v.FISCAL_YEAR as fourin1_dose1_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS fourin1_dose1_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
         --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = '4IN1_1' AND v.EVENT_TYPE = 'Administration'