{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Creating a base table for vaccinations for this population to be joined against in further analysis n~2.5 million rows 
SELECT DISTINCT
p.PERSON_ID
--,p.practice_name as GP_NAME
,p.practice_code
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date), YEAR(v.event_date) - 1)
  || '/'
  || LPAD(RIGHT(TO_VARCHAR(IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date) + 1, YEAR(v.event_date))), 2), 2, '0')
    AS FISCAL_YEAR
FROM {{ ref('dim_person_demographics') }} p
--FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_VACCINATION_EVENTS_HISTORICAL v using (PERSON_ID)
--restrict by AGE to less than 11
WHERE p.age < 11