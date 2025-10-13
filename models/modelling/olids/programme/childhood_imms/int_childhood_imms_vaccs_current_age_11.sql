{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

with VACC11YRBASE as (
SELECT DISTINCT
PERSON_ID
,BIRTH_DATE_APPROX
,AGE
,FIRST_BDAY
,VACCINE_ORDER
,VACCINE_ID
,VACCINATION_STATUS
,VACCINATION_DATE
--use the built in age at event from EMIS
,AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_vaccination_status_current') }}
WHERE AGE = 11
--Replace AGE = 11 for more accurate Age Bucket which is created from actual DOB.base population selected by age only, not relevant vaccinations
--WHERE age_bucket = 'Age 11-12'  
)
-- Creating CTE for 6in1 (dose 1,2,3) where 1 row is per patient at 11 yr AS NUMERATOR
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
    --HELPER COLUMN to check number of months between DOB and vaccination date is not > 132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM VACC11YRBASE v1
    LEFT JOIN VACC11YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 4 AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue')
    LEFT JOIN VACC11YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ORDER = 7 AND v3.VACCINATION_STATUS not in ('Declined', 'Contraindicated' ,'Overdue')
    WHERE v1.VACCINE_ORDER = 1 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )  
)
 -- Creating CTE for 4-in-1 (dose 1) where 1 row is per patient at 11 yr AS NUMERATOR
,FOURIN1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS fourin1_first_date,
        v1.VACCINATION_STATUS as fourin1_first_status,
         v1.AGE_AT_EVENT_OBS as fourin1_first_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date is not > 132 months (unlikely)
   ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS fourin1_event_age_mths
           FROM VACC11YRBASE v1
        WHERE v1.VACCINE_ORDER = 14 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
 -- Creating CTE for HibMenC (dose 1) where 1 row is per patient at 11 yr AS NUMERATOR
,HIBMENC AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS hibmc_first_date,
        v1.VACCINATION_STATUS as hibmc_first_status,
        v1.AGE_AT_EVENT_OBS as hibmc_first_event_age,
--calculate HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS hibmc_first_bday_mths,
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 132 months (unlikely)
       ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS hibmc_event_age_mths
           FROM VACC11YRBASE v1
        WHERE v1.VACCINE_ORDER = 9 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
)  
-- Creating CTE for MMR (dose 1) where 1 row is per patient at 11 yr AS NUMERATOR
,MMR AS ( 
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmr_first_date,
         v1.VACCINATION_STATUS as mmr_first_status,
         v1.AGE_AT_EVENT_OBS as mmr_first_event_age,
--HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,   
--HELPER column number of months between second and first vaccination. Must be >= 3
        ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.VACCINATION_DATE)) as mmr_first_second_mths,
 --HELPER COLUMN to check number of months between DOB and second vaccination date is not >132 months (unlikely)
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_second_event_age_mths,
         v2.VACCINATION_DATE AS mmr_second_date,
        v2.VACCINATION_STATUS AS mmr_second_status,
         v2.AGE_AT_EVENT_OBS as mmr_second_event_age,
          FROM VACC11YRBASE v1
          LEFT JOIN VACC11YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 15 AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
          WHERE v1.VACCINE_ORDER = 11 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
         
) 
,COMBINED AS (
SELECT distinct
    v.PERSON_ID 
    ,CURRENT_DATE as run_date
    ,'11 Years' as reporting_age
    --6-IN-1 ALIGN to HEI current logic - 3 doses anytime before or on eleventh bday
    ,CASE WHEN  (s.sixin1_first_status in ('Completed','OutofSchedule') AND s.sixin1_first_event_age <= 11) AND
     (s.sixin1_second_status in ('Completed','OutofSchedule') AND s.sixin1_second_event_age <= 11) AND
 	(s.sixin1_third_status in ('Completed','OutofSchedule') AND s.sixin1_third_event_age <= 11 AND s.sixin1_third_event_age_mths <= 132) THEN 1
	ELSE 0 END AS sixin1_comp_by_11
--------------------------------
--4-IN-1 ALIGN to HEI current logic - 1 doses anytime before or on eleventh bday - do not restrict by 3years and 4 mths
    ,CASE WHEN (f.fourin1_first_status in ('Completed','OutofSchedule') AND f.fourin1_first_event_age <= 11 AND f.fourin1_event_age_mths <= 132) 
  	THEN 1 ELSE 0 END AS fourin1_comp_by_11  
-------------------------------
 --HIBMENC ALIGN to HEI current logic - 1 dose anytime before or on eleventh_bday and after the age of one year.
    ,CASE WHEN h.hibmc_first_status in ('Completed','OutofSchedule') AND h.hibmc_first_bday_mths >= 0 AND h.hibmc_first_event_age <= 11 AND h.hibmc_event_age_mths <= 132
    THEN 1 ELSE 0 END AS hibmc_comp_by_11
--------------------------------
--MMR Doses 1 and 2 ALIGN to HEI current logic Evaluate whether or not MMR (dose 2) has been completed by the eleventh birthday and after first dose and first dose is on or after first b-day
--and one dose must be at least 15 mths if the first dose is at at 1 year then second dose can be at least 3 mths after Dose 1
   	,CASE WHEN mr.mmr_first_status in ('Completed','OutofSchedule') AND mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0  
    AND mmr_first_second_mths >= 3 AND mr.mmr_second_event_age <=11 AND mr.mmr_second_event_age_mths <=132  THEN 1 ELSE 0 END AS mmr_comp_by_11
FROM VACC11YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join FOURIN1 f using (PERSON_ID) 
left join HIBMENC h using (PERSON_ID)  
left join MMR mr using (PERSON_ID) 
)  
--add back in demographics
select 
c.*
,CASE 
WHEN sixin1_comp_by_11 = 1 AND fourin1_comp_by_11 = 1 AND hibmc_comp_by_11 = 1 AND mmr_comp_by_11 = 1 THEN 1
	ELSE 0 END AS all_comp_by_11
,p.GENDER
,p.AGE
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.ETHNICITY_GRANULAR
,p.IMD_QUINTILE
,p.IMDQUINTILE_ORDER
,p.IMD_DECILE
,p.MAIN_LANGUAGE
,p.PRACTICE_BOROUGH 
,p.PRACTICE_NEIGHBOURHOOD
,p.PRIMARY_CARE_NETWORK
,p.GP_NAME
,p.PRACTICE_CODE
,p.RESIDENTIAL_BOROUGH
,p.RESIDENTIAL_NEIGHBOURHOOD
,p.RESIDENTIAL_LOC
,p.WARD_CODE
,p.WARD_NAME
,p.LAC_FLAG

FROM combined c 
INNER JOIN {{ ref('int_childhood_imms_current_population') }} p using (PERSON_ID) 
