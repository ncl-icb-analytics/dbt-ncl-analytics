{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}


with VACC2YRBASE as (
SELECT DISTINCT
PERSON_ID
,BIRTH_DATE_APPROX
,BORN_SEP_2022_FLAG
,BORN_JUL_2024_FLAG
,BORN_JAN_2025_FLAG
,FIRST_BDAY
,SECOND_BDAY
,VACCINE_ORDER
,VACCINE_ID
,VACCINATION_STATUS
,VACCINATION_DATE
--use the built in age at event from EMIS
,AGE_AT_EVENT
FROM {{ ref('int_childhood_imms_vaccination_status_current') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_VACCINATION_STATUS_CURRENT
--Children that are currently aged 2 (based on approx dob) base population selected by age only, not relevant vaccinations
WHERE AGE = 2
--Replace AGE = 2 for more accurate Age Bucket which is created from actual DOB
--WHERE age_bucket = 'Age 2-3' 
)
-- Creating CTE for 6in1 (dose 1,2,3) born before 1st July 2024
,SIXIN1 AS (
       SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS sixin1_first_date, 
        v1.VACCINATION_STATUS AS sixin1_first_status,
	v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.VACCINATION_DATE AS sixin1_second_date,
        v2.VACCINATION_STATUS AS sixin1_second_status,
	v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.VACCINATION_DATE AS sixin1_third_date,
        v3.VACCINATION_STATUS AS sixin1_third_status,
	v3.AGE_AT_EVENT as sixin1_third_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM VACC2YRBASE v1
    LEFT JOIN VACC2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    LEFT JOIN VACC2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    WHERE v1.VACCINE_ID = '6IN1_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule' ) 
    AND v1.BORN_JUL_2024_FLAG = 'No'
    
)
-- Creating CTE for 6in1 (dose 1,2,3,4) born on or after 1st July 2024
,SIXIN1B AS (
       SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS sixin1_first_date, 
        v1.VACCINATION_STATUS AS sixin1_first_status,
	   v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.VACCINATION_DATE AS sixin1_second_date,
        v2.VACCINATION_STATUS AS sixin1_second_status,
	   v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.VACCINATION_DATE AS sixin1_third_date,
        v3.VACCINATION_STATUS AS sixin1_third_status,
	   v3.AGE_AT_EVENT as sixin1_third_event_age,
       --HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths,
       v4.VACCINATION_DATE AS sixin1_fourth_date,
        v4.VACCINATION_STATUS AS sixin1_fourth_status,
    v4.AGE_AT_EVENT as sixin1_fourth_event_age,
    --HELPER COLUMN to check number of months between DOB and 4th vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v4.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_fourth_event_age_mths
    FROM VACC2YRBASE v1
    LEFT JOIN VACC2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    LEFT JOIN VACC2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    LEFT JOIN VACC2YRBASE v4 ON v1.PERSON_ID = v4.PERSON_ID AND v4.VACCINE_ID = '6IN1_4' AND v4.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    WHERE v1.VACCINE_ID = '6IN1_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
    AND v1.BORN_JUL_2024_FLAG = 'Yes'
)
 -- Creating CTE for HibMenC (dose 1) where 1 row is per patient AS NUMERATOR
,HIBMENC AS (
    SELECT 
         v1.PERSON_ID, 
         v1.VACCINATION_DATE AS hibmc_first_date,
         v1.VACCINATION_STATUS as hibmc_first_status,
	   v1.AGE_AT_EVENT as hibmc_first_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS hibmc_event_age_mths
         FROM VACC2YRBASE v1
        WHERE v1.VACCINE_ID = 'HIBMENC_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule')
)  
-- Creating CTE for MMR (dose 1) for those born on or after 1st September OR on or after 1st July 2024 - receive single dose of MMR by the age of 2 
,MMR1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmr_first_date,
        v1.VACCINATION_STATUS as mmr_first_status,
        v1.AGE_AT_EVENT as mmr_first_event_age,
--calculate HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,
 --HELPER COLUMN to check number of months between DOB and vaccination date to check not > 24 months
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_first_event_age_mths
        FROM VACC2YRBASE v1
        WHERE v1.VACCINE_ID = 'MMR_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
        AND (v1.BORN_SEP_2022_FLAG = 'Yes' OR v1.BORN_JUL_2024_FLAG = 'Yes')
) 
-- Creating new CTE for MMRV (dose 1) when born on or after 1st July 2024 they receive MMRV as MMR Dose 2 at 18 months
,MMRV1B AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmrv_first_date,
         v1.VACCINATION_STATUS as mmrv_first_status,
        v1.AGE_AT_EVENT as mmrv_first_event_age,
--calculate HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmrv_first_bday_mths,
         --HELPER COLUMN to check number of months between DOB and vaccination date is not >24 months
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_first_event_age_mths
          FROM VACC2YRBASE v1
          WHERE v1.VACCINE_ID = 'MMRV_1B' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
          AND v1.BORN_JUL_2024_FLAG = 'Yes'
) 
-- Creating CTE children born on or after 1st January 2025 - receive two doses of MMRV by the age of two. First at age 1 and 2nd dose at 18 months
,MMRV1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmrv_first_date,
         v1.VACCINATION_STATUS as mmrv_first_status,
        v1.AGE_AT_EVENT as mmrv_first_event_age,
--calculate HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmrv_first_bday_mths,
         --HELPER COLUMN to check number of months between DOB and vaccination date is not >24 months
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_first_event_age_mths,
         v2.VACCINATION_DATE AS mmrv_second_date,
        v2.VACCINATION_STATUS AS mmrv_second_status,
        v2.AGE_AT_EVENT as mmrv_second_event_age,
        --HELPER COLUMN to check number of months between DOB and second vaccination date is not >24 months
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_second_event_age_mths
          FROM VACC2YRBASE v1
          LEFT JOIN VACC2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MMRV_2' AND v2.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
          WHERE v1.VACCINE_ID = 'MMRV_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule' )
        AND v1.BORN_JAN_2025_FLAG = 'Yes'
)
-- Creating CTE for MenB (dose 1 and 2 and booster) where 1 row is per patient AS NUMERATOR
,MENB AS (
    SELECT 
    v1.PERSON_ID, 
    v1.VACCINATION_DATE AS menb_first_date, 
    v1.VACCINATION_STATUS AS menb_first_status,
	v1.AGE_AT_EVENT as menb_first_event_age,
    v2.VACCINATION_DATE AS menb_second_date,
    v2.VACCINATION_STATUS AS menb_second_status,
v2.AGE_AT_EVENT as menb_second_event_age,
    v3.VACCINATION_DATE AS menb_third_date,
    v3.VACCINATION_STATUS AS menb_third_status,
v3.AGE_AT_EVENT as menb_third_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS menb_third_event_age_mths
    FROM VACC2YRBASE v1
    LEFT JOIN VACC2YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MENB_2' AND v2.VACCINATION_STATUS in ('Completed', 'OutofSchedule')
    LEFT JOIN VACC2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = 'MENB_3' AND v3.VACCINATION_STATUS in ('Completed', 'OutofSchedule')
    WHERE v1.VACCINE_ID = 'MENB_1' AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule')
)
-- Creating CTE for PCV (dose 1 and 2) where 1 row is per patient AS NUMERATOR
,PCV AS (
    SELECT 
        v1.PERSON_ID,  
        v1.VACCINATION_DATE AS pcv_first_date,
        v1.VACCINATION_STATUS AS pcv_first_status,
        v1.AGE_AT_EVENT as pcv_first_event_age,
        v2.VACCINATION_DATE AS pcv_second_date,
        v2.VACCINATION_STATUS AS pcv_second_status,
        v2.AGE_AT_EVENT as pcv_second_event_age,
--calculate HELPER column number of months between second vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.FIRST_BDAY)) AS pcv_second_first_bday_mths
         FROM VACC2YRBASE v1
         LEFT JOIN VACC2YRBASE v2 
         ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'PCV_2' AND v2.VACCINATION_STATUS in ('Completed', 'OutofSchedule')
         WHERE v1.VACCINE_ID = 'PCV_1'  
         --AND v1.VACCINATION_STATUS in ('Completed', 'OutofSchedule')allow for missing first dose
) 
,COMBINED AS (
SELECT distinct
    v.PERSON_ID 
    ,CURRENT_DATE as run_date
    ,'2 Years' as reporting_age
--6-IN-1 3 doses anytime before or on second bday. THIRD sixin1 AGE AT EVENT must be less than or equal to 2 and check that event age in mths is <= 24 mths 
    ,CASE WHEN  s.sixin1_first_event_age <= 2 AND s.sixin1_second_event_age <= 2 AND s.sixin1_third_event_age <= 2 AND s.sixin1_third_event_age_mths <= 24 THEN 1
        WHEN  s1.sixin1_first_event_age <= 2 AND s1.sixin1_second_event_age <= 2 AND s1.sixin1_third_event_age <= 2 AND s1.sixin1_third_event_age_mths <= 24 THEN 1
	ELSE 0 end as sixin1_comp_by_2
--6-IN-1 4th Dose at 18 mths. AGE AT EVENT must be less than or equal to 2 and check that event age in mths is BETWEEN 18 AND 24 mths for those born on or after 1st July 2024. Fourth dose at 18mths to replace HIBMenC
,CASE WHEN  s1.sixin1_first_event_age <= 2 AND s1.sixin1_second_event_age <= 2 AND s1.sixin1_third_event_age <= 2 AND s1.sixin1_fourth_event_age <= 2
    AND s1.sixin1_fourth_event_age_mths between 18 AND 24 THEN 1 ELSE 0 end as sixin1_4_comp_by_2
--HIBMENC 1 dose anytime before or on second bday - this will be phased out with 6in1 Dose 4 for those born on or after 1st July 2024
    --hibmenc AGE AT EVENT must be less than or equal to 2 and check that event age in mths is <= 24 mths (EMIS sometimes rounds up and sometimes rounds down)
   ,CASE WHEN h.hibmc_first_event_age <= 2 AND h.hibmc_event_age_mths <= 24 THEN 1 ELSE 0  END AS hibmc_comp_by_2
--MMR Dose 1 on or after their first birthday and before or on second birthday for those born on or after 1st September 2022 OR on or after 1st July 2024
--MMR dose 1 AGE AT EVENT years must be at least 1 but <3 and first dose must not be before FIRST_BDAY (ie mmr1_first_bday_mths is negative)
    ,CASE WHEN mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0 AND mmr_first_event_age_mths <=24 THEN 1 ELSE 0 END AS mmr1_comp_by_2
--MMRV Dose 1 at 18 mths and before or on second birthday for those born on or after 1st July 2024 OR on or after their first birthday and before or on second birthday for those born on or after 1st Jan 2025
--MMRV dose 1 AGE AT EVENT years must be at least 1 but <3 and first dose must not be before FIRST_BDAY (ie mmr1_first_bday_mths is negative)
    ,CASE WHEN mv1b.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1b.mmrv_first_bday_mths >= 0 AND mv1b.mmrv_first_event_age_mths between 18 AND 24 THEN 1 
            WHEN mv1.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1.mmrv_first_bday_mths >= 0 AND mv1.mmrv_first_event_age_mths <=24 THEN 1 
    ELSE 0 END AS mmrv1_comp_by_2
--MMRV Doses 1 and 2 Evaluate whether or not MMRV (dose 2) has been completed between 18 mths and 2 years and after first dose for those born on or after 1st Jan 2025
     ,CASE WHEN mv1.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1.mmrv_first_bday_mths >= 0 AND mv1.mmrv_second_date >= mv1.mmrv_first_date 
  AND mv1.mmrv_second_event_age <=2 AND mv1.mmrv_second_event_age_mths between 18 AND 24 THEN 1 ELSE 0 END AS mmrv2_comp_by_2
--MenB 3rd dose anytime before or on second bday
     --THIRD menB AGE AT EVENT must be less than or equal to 2 and check that event age in mths is <= 24 mths
    ,CASE 
     WHEN m.menb_third_event_age <= 2 AND m.menb_third_event_age_mths <=24 THEN 1
     ELSE 0 END AS menb_comp_by_2
--PCV Dose 2 >= 4wks after Dose 1 & >=1st_bday & <= 2nd_bday OR Dose1 is null & Dose 2 >=1st_bday & <= 2nd_bday
    --PCV 2nd dose age at event years must be at least 1 but <3 and 2nd dose must not be before FIRST_BDAY (ie pcv_second_first_bday_mths is negative)
  ,CASE WHEN p.pcv_second_event_age BETWEEN 1 AND 2 AND pcv_second_first_bday_mths >= 0 AND FLOOR(DATEDIFF('day', p.pcv_first_date, p.pcv_second_date) / 7) >= 4 THEN 1
  WHEN p.pcv_first_date IS NULL AND p.pcv_second_event_age BETWEEN 1 AND 2 AND pcv_second_first_bday_mths >= 0 THEN 1 ELSE 0 END AS pcv_comp_by_2   
FROM VACC2YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join SIXIN1B s1 using (PERSON_ID)
left join HIBMENC h using (PERSON_ID)   
left join MMR1 mr using (PERSON_ID) 
left join MMRV1B mv1b using (PERSON_ID)
left join MMRV1 mv1 using (PERSON_ID)
left join MENB m using (PERSON_ID) 
left join PCV p using (PERSON_ID)    
)  
--add back in demographics
select 
c.*
,CASE 
	WHEN sixin1_comp_by_2 = 1 AND hibmc_comp_by_2 = 1 AND mmr1_comp_by_2 = 1 AND menb_comp_by_2 = 1 AND pcv_comp_by_2 = 1 THEN 1
	ELSE 0 END AS all_comp_by_2
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
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_CURRENT_POPULATION p using (PERSON_ID)