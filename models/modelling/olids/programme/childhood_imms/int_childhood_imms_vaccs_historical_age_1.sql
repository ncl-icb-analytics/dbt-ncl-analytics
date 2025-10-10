{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH HIST1YRBASE AS (
select distinct
p.PERSON_ID
,p.practice_name
,p.age
,p.BIRTH_DATE_APPROX
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,v.AGE_AT_EVENT_OBS as AGE_AT_EVENT
FROM {{ ref('int_childhood_imms_historical_population') }} p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--restrict by AGE not by VACCINE as for currently aged 1 - otherwise base population is not correct
where p.age = 1 
)

-- Creating CTE for 6in1 (dose 1,2,3) where 1 row is per patient AS NUMERATOR
,SIXIN1 AS (
       SELECT 
         v1.PERSON_ID, 
        v1.EVENT_DATE AS sixin1_first_date, 
        v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.EVENT_DATE AS sixin1_second_date,
        v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.EVENT_DATE AS sixin1_third_date,
        v3.AGE_AT_EVENT as sixin1_third_event_age,
    --HELPER COLUMN to check number of months between DOB and 3rd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM HIST1YRBASE v1
    LEFT JOIN HIST1YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 4 AND v2.EVENT_TYPE = 'Administration'
    LEFT JOIN HIST1YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ORDER = 7 AND v3.EVENT_TYPE = 'Administration'
    WHERE v1.VACCINE_ORDER = 1  AND v1.EVENT_TYPE = 'Administration'
)

 -- Creating CTE for Rotavirus (dose 1 and 2) where 1 row is per patient AS NUMERATOR
,ROTA AS (
    SELECT 
       v1.PERSON_ID, 
        v1.EVENT_DATE AS rota_first_date, 
        v1.AGE_AT_EVENT as  rota_first_event_age,
        v2.EVENT_DATE AS rota_second_date,
        v2.AGE_AT_EVENT as  rota_second_event_age,
        --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS rota_second_event_age_mths
    FROM HIST1YRBASE v1
    LEFT JOIN HIST1YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 6 AND v2.EVENT_TYPE = 'Administration'
    WHERE v1.VACCINE_ORDER = 3 AND v1.EVENT_TYPE = 'Administration'
)  
-- Creating CTE for MenB (dose 1 and 2 and booster) where 1 row is per patient AS NUMERATOR
,MENB AS (
    SELECT 
      v1.PERSON_ID, 
          v1.EVENT_DATE AS menb_first_date, 
          v1.AGE_AT_EVENT as menb_first_event_age,
         v2.EVENT_DATE AS menb_second_date,
         v2.AGE_AT_EVENT as menb_second_event_age,
    --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS menb_second_event_age_mths
    FROM HIST1YRBASE v1
    LEFT JOIN HIST1YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 8 AND v2.EVENT_TYPE = 'Administration'
    WHERE v1.VACCINE_ORDER = 2 and v1.EVENT_TYPE = 'Administration'
   )
-- Creating CTE for PCV (dose 1 and 2) where 1 row is per patient AS NUMERATOR this is relevant for infants born on or after January 1, 2020
,PCV AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS pcv_first_date,
        v1.AGE_AT_EVENT as pcv_first_event_age,
    --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS pcv_first_event_age_mths
         FROM HIST1YRBASE v1
       WHERE v1.VACCINE_ORDER = 5 AND v1.EVENT_TYPE = 'Administration'
       AND v1.BIRTH_DATE_APPROX >= '2020-01-16'
         )
,COMBINED AS (
SELECT DISTINCT
v.PERSON_ID 
,v.PRACTICE_NAME
,v.AGE
--SIX-IN-1 all 3 doses must be less than 2 on event date
  ,CASE WHEN s.sixin1_first_event_age <= 1 AND s.sixin1_second_event_age <= 1 AND s.sixin1_third_event_age <= 1 
    AND s.sixin1_third_event_age_mths <= 12 THEN 1  ELSE 0 end as sixin1_comp_by_1
--ROTAVIRUS 2 doses anytime before or on first bday ie age at event <=1 and check that second Rota age at event in months <=12
      ,CASE WHEN r.rota_first_event_age <= 1 AND r.rota_second_event_age <= 1 AND r.rota_second_event_age_mths <= 12  THEN 1
		 ELSE 0 end as rota_comp_by_1
  --MENB 2 doses must be less than 1 on event date
    ,CASE WHEN m.menb_first_event_age <= 1 AND m.menb_second_event_age <= 1 AND m.menb_second_event_age_mths <= 12  THEN 1
		 else 0 end as menb_comp_by_1
--PCV 1 dose anytime before or on first bday
    ,CASE WHEN p.pcv_first_event_age <= 1 AND p.pcv_first_event_age_mths <=12 THEN 1 ELSE 0 END AS pcv_comp_by_1  
FROM HIST1YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join ROTA r using (PERSON_ID)   
left join MENB m using (PERSON_ID) 
left join PCV p using (PERSON_ID) 
)
,FINAL_PREJOIN AS (
SELECT 
person_id
,practice_name
,age
,SIXIN1_COMP_BY_1
,ROTA_COMP_BY_1
,MENB_COMP_BY_1
,PCV_COMP_BY_1
,CASE 
	WHEN sixin1_comp_by_1 = 1 AND rota_comp_by_1 = 1 AND menb_comp_by_1 = 1 AND pcv_comp_by_1 = 1 THEN 1
	ELSE 0 END AS all_comp_by_1
FROM COMBINED
)

--add back in demographics and person months
select 
p.ANALYSIS_MONTH
,v.PERSON_ID
,v.PRACTICE_NAME
,p.practice_code
,v.AGE
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,v.sixin1_comp_by_1
,v.rota_comp_by_1
,v.menb_comp_by_1
,v.pcv_comp_by_1
,v.all_comp_by_1
FROM {{ ref('int_childhood_imms_historical_population') }} p 
LEFT JOIN FINAL_PREJOIN v
 on p.person_id = v.person_id and p.age= v.age and p.practice_name = v.practice_name
 where p.age = 1
order by 1,2