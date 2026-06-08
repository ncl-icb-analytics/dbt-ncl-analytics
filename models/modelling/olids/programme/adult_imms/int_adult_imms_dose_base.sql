{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

--Using the person_demographics for anyone over the age of 60 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Creating a base table for vaccinations for this population to be joined against in further analysis n~2.5 million rows 
SELECT DISTINCT
p.PERSON_ID
--adding age to cross checks against FDP figures.
,p.AGE
,p.AGE_BAND_5Y
,p.practice_code
,CASE WHEN (ch.PERSON_ID IS NOT NULL OR cch.PERSON_ID IS NOT NULL) THEN TRUE ELSE FALSE END AS IS_CARE_HOME_RESIDENT
,v.VACCINE_ID
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date), YEAR(v.event_date) - 1)
  || '/'
  || LPAD(RIGHT(TO_VARCHAR(IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date) + 1, YEAR(v.event_date))), 2), 2, '0')
    AS FISCAL_YEAR
FROM {{ ref('dim_person_demographics') }} p
--FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS p
LEFT JOIN {{ ref('int_adult_imms_vaccination_events_historical') }} v using (PERSON_ID)
LEFT JOIN {{ ref('dim_person_care_home') }} ch using (PERSON_ID)
LEFT JOIN {{ ref('int_covid_long_term_residential_care') }} cch using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_EVENTS_HISTORICAL v using (PERSON_ID)
--restrict by AGE to 65 or over
WHERE p.age >= 65