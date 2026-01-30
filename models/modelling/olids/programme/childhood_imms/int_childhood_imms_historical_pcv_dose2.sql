{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Capture the pcv dose 2 vaccination events for this population
  SELECT 
        v.PERSON_ID
        ,v.practice_code
          ,v.EVENT_DATE AS pcv_dose2_date 
        ,TO_NUMBER(TO_CHAR(v.event_date, 'YYYYMM')) AS pcv_dose2_sort
        ,v.FISCAL_YEAR as pcv_dose2_fiscal
        ,MONTHNAME(v.EVENT_DATE) || '-' || YEAR(v.EVENT_DATE) AS pcv_dose2_label
        FROM {{ ref('int_childhood_imms_dose_base_child') }} v
         --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
        --restrict to administered doses only
       WHERE v.VACCINE_ID = 'PCV_2' AND v.EVENT_TYPE = 'Administration'