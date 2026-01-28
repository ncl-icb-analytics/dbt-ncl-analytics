{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the historic population anyone under the age of 111 who has been registered with an NCL practice in the last 48 months Rolling
--Creating a base table for vaccinations
SELECT DISTINCT
p.PERSON_ID
,p.practice_name as GP_NAME
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
FROM {{ ref('int_childhood_imms_historical_population') }} p
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_POPULATION p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_VACCINATION_EVENTS_HISTORICAL v using (PERSON_ID)
--restrict by AGE to less than 11
WHERE p.age < 11