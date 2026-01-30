{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the menb dose 3 vaccination events for this population
  SELECT 
        v.PERSON_ID
        ,v.practice_code
        ,v.EVENT_DATE AS menb_dose3_date 
       ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS menb_dose3_sort
       , v.FISCAL_YEAR as menb_dose3_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS menb_dose3_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
         --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
        --restrict to administered doses only
        WHERE v.VACCINE_ID = 'MENB_3' AND v.EVENT_TYPE = 'Administration'