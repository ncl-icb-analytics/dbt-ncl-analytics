{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH HIST11YRBASE AS (
select distinct
p.PERSON_ID
,p.practice_name
,p.age
,p.BIRTH_DATE_APPROX
,p.FIRST_BDAY
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,v.AGE_AT_EVENT_OBS as AGE_AT_EVENT
FROM {{ ref('int_childhood_imms_historical_population') }} p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--restrict by AGE not by VACCINE as for currently aged 11 - otherwise base population is not correct
where p.age = 11

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
    --HELPER COLUMN to check number of months between DOB and vaccination date to check not > 132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM HIST11YRBASE v1
    LEFT JOIN HIST11YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 4 AND v2.EVENT_TYPE = 'Administration'
    LEFT JOIN HIST11YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ORDER = 7 AND v3.EVENT_TYPE = 'Administration'
    WHERE v1.VACCINE_ORDER = 1  AND v1.EVENT_TYPE = 'Administration'
)

-- Creating CTE for HibMenC (dose 1) where 1 row is per patient AS NUMERATOR
,FOURIN1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS fourin1_first_date,
        v1.AGE_AT_EVENT as fourin1_first_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS fourin1_event_age_mths
         FROM HIST11YRBASE v1
        WHERE v1.VACCINE_ORDER = 14 and v1.EVENT_TYPE = 'Administration'
)  

 -- Creating CTE for HibMenC (dose 1) where 1 row is per patient at 5 yr AS NUMERATOR
,HIBMENC AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS hibmc_first_date,
        v1.AGE_AT_EVENT as hibmc_first_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date is not > 132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS hibmc_event_age_mths
        FROM HIST11YRBASE v1
        WHERE v1.VACCINE_ORDER = 9 AND v1.EVENT_TYPE = 'Administration'
)

-- Creating CTE for MMR (dose 1 & Dose 2) where 1 row is per patient AS NUMERATOR
,MMR AS (
        SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS mmr_first_date,
        v1.AGE_AT_EVENT as mmr_first_event_age,
--HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,   
--HELPER column number of months between first vaccination and second vaccination. Must be >= 3
        ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.EVENT_DATE)) as mmr_first_second_mths,
 --HELPER COLUMN to check number of months between DOB and second vaccination date is not >132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_second_event_age_mths,
         v2.EVENT_DATE AS mmr_second_date,
         v2.AGE_AT_EVENT as mmr_second_event_age,
         FROM HIST11YRBASE v1
        LEFT JOIN HIST11YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 15 and v2.EVENT_TYPE = 'Administration'
        WHERE v1.VACCINE_ORDER = 11 and v1.EVENT_TYPE = 'Administration'
) 

,COMBINED AS (
SELECT DISTINCT
v.PERSON_ID 
,v.PRACTICE_NAME
,v.AGE
--all 3 doses must be less than 2 on event date
     ,CASE WHEN s.sixin1_first_event_age <= 11 AND s.sixin1_second_event_age <= 11 AND s.sixin1_third_event_age <= 11 AND s.sixin1_third_event_age_mths <= 132 
     THEN 1 ELSE 0 end as sixin1_COMP_BY_11
--4-IN-1 ALIGN to HEI current logic - 1 doses anytime before or on fifth bday ie AGE AT EVENT must be less than or equal to 5 and check that event age in mths is <= 132 mths
    ,CASE WHEN f.fourin1_first_event_age <= 11 AND f.fourin1_event_age_mths <= 132 THEN 1 ELSE 0 END AS fourin1_COMP_BY_11
--HIBMENC must be less than or equal to 2 on event date and check that event age in mths is <= 114 mths (EMIS sometimes rounds up and sometimes rounds down)
   ,CASE WHEN h.hibmc_first_event_age <= 11 AND h.hibmc_event_age_mths <= 132 THEN 1 ELSE 0  END AS hibmc_COMP_BY_11
--MMR Doses 1 and 2 Evaluate whether or not MMR (dose 2) has been completed by the eleventh birthday and after first dose and first dose is on or after first b-day
--and one dose must be at least 15 mths if the first dose is at at 1 year then second dose can be at least 3 mths after Dose 1
   	,CASE WHEN mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0  AND mmr_first_second_mths >= 3 
    AND mr.mmr_second_event_age <=11 AND mr.mmr_second_event_age_mths <=132  THEN 1 ELSE 0 END AS mmr_comp_by_11
 
FROM HIST11YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join FOURIN1 f using (PERSON_ID) 
left join HIBMENC h using (PERSON_ID)  
left join MMR mr using (PERSON_ID) 
)
,FINAL_PREJOIN AS (
SELECT 
person_id
,practice_name
,age
,sixin1_COMP_BY_11
,fourin1_COMP_BY_11
,hibmc_COMP_BY_11
,mmr_COMP_BY_11
,CASE 
	WHEN sixin1_comp_by_11 = 1 AND fourin1_comp_by_11 = 1 AND hibmc_comp_by_11 = 1 AND mmr_comp_by_11 = 1 THEN 1
	ELSE 0 END AS all_COMP_BY_11
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
,v.sixin1_COMP_BY_11
,v.fourin1_COMP_BY_11
,v.hibmc_COMP_BY_11
,v.mmr_COMP_BY_11
,v.all_COMP_BY_11
FROM {{ ref('int_childhood_imms_historical_population') }} p 
LEFT JOIN FINAL_PREJOIN v
 on p.person_id = v.person_id and p.age= v.age and p.practice_name = v.practice_name
 where p.age = 11
order by 1,2