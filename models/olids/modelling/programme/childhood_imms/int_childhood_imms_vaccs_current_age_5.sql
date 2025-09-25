{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

with VACC5YRBASE as (
SELECT DISTINCT
PERSON_ID
,BIRTH_DATE_APPROX
,AGE
,FIRST_BDAY
,VACCINE_ORDER
,VACCINATION_STATUS
,VACCINATION_DATE
--use the built in age at event from EMIS
,AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_vaccination_status_current') }}
WHERE AGE = 5
--Replace AGE = 5 for more accurate Age Bucket which is created from actual DOB. base population selected by age only, not relevant vaccinations
--WHERE age_bucket = 'Age 5-6'  
)
-- Creating CTE for 6in1 (dose 1,2,3) where 1 row is per patient AS NUMERATOR
,SIXIN1 AS (
       SELECT 
         v1.PERSON_ID, 
        v1.VACCINATION_DATE AS sixin1_first_date, 
        v1.VACCINATION_STATUS AS sixin1_first_status,
	   v1.AGE_AT_EVENT_OBS as sixin1_first_event_age,
        v2.VACCINATION_DATE AS sixin1_second_date,
        v2.VACCINATION_STATUS AS sixin1_second_status,
	   v2.AGE_AT_EVENT_OBS as sixin1_second_event_age,
        v3.VACCINATION_DATE AS sixin1_third_date,
        v3.VACCINATION_STATUS AS sixin1_third_status,
	   v3.AGE_AT_EVENT_OBS as sixin1_third_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date is not >60 months
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM VACC5YRBASE v1
    LEFT JOIN VACC5YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 4 AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue')
    LEFT JOIN VACC5YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ORDER = 7 AND v3.VACCINATION_STATUS not in ('Declined', 'Contraindicated' ,'Overdue')
    WHERE v1.VACCINE_ORDER = 1 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )  
)
 -- Creating CTE for 4-in-1 (dose 1) where 1 row is per patient at 5 yr AS NUMERATOR
,FOURIN1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS fourin1_first_date,
        v1.VACCINATION_STATUS as fourin1_first_status,
        v1.AGE_AT_EVENT_OBS as fourin1_first_event_age,
         --HELPER COLUMN to check number of months between DOB and vaccination date is not >60 months
    ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS fourin1_event_age_mths
           FROM VACC5YRBASE v1
        WHERE v1.VACCINE_ORDER = 14 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
 -- Creating CTE for HibMenC (dose 1) where 1 row is per patient at 5 yr AS NUMERATOR
,HIBMENC AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS hibmc_first_date,
        v1.VACCINATION_STATUS as hibmc_first_status,
        v1.AGE_AT_EVENT_OBS as hibmc_first_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date is not >60 months
    ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS hibmc_event_age_mths
           FROM VACC5YRBASE v1
        WHERE v1.VACCINE_ORDER = 9 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
)  
-- Creating CTE for MMR (dose 1) where 1 row is per patient at 5 yr AS NUMERATOR
,MMR AS ( 
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmr_first_date,
         v1.VACCINATION_STATUS as mmr_first_status,
          v1.AGE_AT_EVENT_OBS as mmr_first_event_age,
--calculate HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,
         --HELPER COLUMN to check number of months between DOB and vaccination date is not >60 months
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_first_event_age_mths,
         v2.VACCINATION_DATE AS mmr_second_date,
        v2.VACCINATION_STATUS AS mmr_second_status,
        v2.AGE_AT_EVENT_OBS as mmr_second_event_age,
        --HELPER COLUMN to check number of months between DOB and second vaccination date is not >60 months
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_second_event_age_mths
          FROM VACC5YRBASE v1
          LEFT JOIN VACC5YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 15 AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
          WHERE v1.VACCINE_ORDER = 11 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
,COMBINED AS (
SELECT distinct
    v.PERSON_ID 
    ,CURRENT_DATE as run_date
    ,'5 Years' as reporting_age
       --6-IN-1 ALIGN to HEI current logic - 3 doses anytime before or on fifth bday ie THIRD sixin1 AGE AT EVENT must be less than or equal to 5 and check that event age in mths is <= 60 mths
     ,CASE WHEN  (s.sixin1_first_status in ('Completed','OutofSchedule') AND s.sixin1_first_event_age <= 5) AND
     (s.sixin1_second_status in ('Completed','OutofSchedule') AND s.sixin1_second_event_age <= 5) AND
 	(s.sixin1_third_status in ('Completed','OutofSchedule') AND s.sixin1_third_event_age <= 5 AND s.sixin1_third_event_age_mths <= 60) THEN 1
	ELSE 0 END AS sixin1_comp_by_5
--4-IN-1 ALIGN to HEI current logic - 1 doses anytime before or on fifth bday ie AGE AT EVENT must be less than or equal to 5 and check that event age in mths is <= 60 mths
      ,CASE WHEN (f.fourin1_first_status in ('Completed','OutofSchedule') AND f.fourin1_first_event_age <= 5 AND f.fourin1_event_age_mths <= 60) 
    THEN 1 ELSE 0 END AS fourin1_comp_by_5
--HIBMENC ALIGN to HEI current logic - 1 doses anytime before or on fifth bday ie AGE AT EVENT must be less than or equal to 5 and check that event age in mths is <= 60 mths 
    ,CASE WHEN (h.hibmc_first_status in ('Completed','OutofSchedule') AND h.hibmc_first_event_age <= 5 AND h.hibmc_event_age_mths <= 60) THEN 1 ELSE 0  
     END AS hibmc_comp_by_5
--MMR Dose 1 ALIGN to HEI current logic one dose of MMR on or after their first birthday MMR dose AGE AT EVENT years must be at least 1 but <3 and first dose must not be before FIRST_BDAY (ie mmr1_first_bday_mths is negative)
     ,CASE WHEN mr.mmr_first_status in ('Completed','OutofSchedule') AND mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0 
     AND mr.mmr_first_event_age_mths <=60 THEN 1 ELSE 0 END AS mmr1_comp_by_5
--MMR Doses 1 and 2 ALIGN to HEI current logic Evaluate whether or not MMR (dose 2) has been completed by the fifth birthday and after first dose and first dose is on or after first b-day
     ,CASE 
   WHEN mr.mmr_first_status in ('Completed','OutofSchedule') AND mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0 
  AND mr.mmr_second_status in ('Completed','OutofSchedule') AND mr.mmr_second_date >= mr.mmr_first_date 
  AND mr.mmr_second_event_age <=5 AND mr.mmr_second_event_age_mths <=60 THEN 1 ELSE 0 END AS mmr2_comp_by_5
FROM VACC5YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join FOURIN1 f using (PERSON_ID) 
left join HIBMENC h using (PERSON_ID)  
left join MMR mr using (PERSON_ID) 
)  
--add back in demographics
select 
c.*
,CASE 
WHEN sixin1_comp_by_5 = 1 AND fourin1_comp_by_5 = 1 AND hibmc_comp_by_5 = 1 AND mmr1_comp_by_5 = 1 AND mmr2_comp_by_5 = 1 THEN 1
	ELSE 0 END AS all_comp_by_5
,p.GENDER
,p.AGE
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.ETHNICITY_GRANULAR
,p.IMD_QUINTILE
,NULL AS IMDQUINTILE_ORDER
,p.IMD_DECILE
,p.MAIN_LANGUAGE
,p.PRACTICE_BOROUGH 
,p.PRACTICE_NEIGHBOURHOOD
,p.PRIMARY_CARE_NETWORK
,p.GP_NAME
,p.PRACTICE_CODE
,NULL as RESIDENTIAL_BOROUGH
,NULL as RESIDENTIAL_NEIGHBOURHOOD
,NULL as RESIDENTIAL_LOC
,p.WARD_CODE
,p.WARD_NAME
,p.LAC_FLAG

FROM combined c 
INNER JOIN {{ ref('int_childhood_imms_current_population') }} p using (PERSON_ID) 