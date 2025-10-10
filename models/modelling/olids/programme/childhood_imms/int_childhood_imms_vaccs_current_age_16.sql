{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}


with VACC16YRBASE as (
SELECT DISTINCT
PERSON_ID
,BIRTH_DATE_APPROX
,AGE
,FIRST_BDAY
,TWELFTH_BDAY
,THIRTEENTH_BDAY
,VACCINE_ORDER
,VACCINATION_STATUS
,VACCINATION_DATE
--use the built in age at event from EMIS
,AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_vaccination_status_current') }}
WHERE AGE = 16
--Replace AGE = 16 for more accurate Age Bucket which is created from actual DOB. base population selected by age only, not relevant vaccinations
--WHERE age_bucket = 'Age 16-17' 
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
    --HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months (16 years)
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM VACC16YRBASE v1
    LEFT JOIN VACC16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 4 AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue')
    LEFT JOIN VACC16YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ORDER = 7 AND v3.VACCINATION_STATUS not in ('Declined', 'Contraindicated' ,'Overdue')
    WHERE v1.VACCINE_ORDER = 1 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )  
)
 -- Creating CTE for 4-in-1 (dose 1) where 1 row is per patient at 16 yr AS NUMERATOR
,FOURIN1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS fourin1_first_date,
        v1.VACCINATION_STATUS as fourin1_first_status,
        v1.AGE_AT_EVENT_OBS as fourin1_first_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months (16 years)
   ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS fourin1_event_age_mths
           FROM VACC16YRBASE v1
        WHERE v1.VACCINE_ORDER = 14 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
 -- Creating CTE for 3-in-1 (dose 1) where 1 row is per patient AS NUMERATOR
,THREEIN1 AS (
    SELECT 
        v1.PERSON_ID
        ,v1.VACCINATION_DATE AS threein1_first_date
		,v1.VACCINATION_STATUS as threein1_first_status
        ,v1.AGE_AT_EVENT_OBS as threein1_first_event_age
--HELPER column number of months between vaccination date and approx 13th bday. If it's a negative number than the vaccination is early and not valid
        ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.THIRTEENTH_BDAY)) AS threein1_thirteenth_bday_mths   
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months (16 years)
        ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS threein1_event_age_mths
        FROM VACC16YRBASE v1
        WHERE v1.VACCINE_ORDER = 18 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
)
-- Creating CTE for MMR (dose 1) where 1 row is per patient at 5 yr AS NUMERATOR
,MMR AS ( 
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS mmr_first_date,
         v1.VACCINATION_STATUS as mmr_first_status,
         v1.AGE_AT_EVENT_OBS as mmr_first_event_age,
--HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,   
 --HELPER COLUMN to check number of months between DOB and second vaccination date is not > 192 months (16 years)
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_second_event_age_mths,
         v2.VACCINATION_DATE AS mmr_second_date,
        v2.VACCINATION_STATUS AS mmr_second_status,
         v2.AGE_AT_EVENT_OBS as mmr_second_event_age,
          FROM VACC16YRBASE v1
          LEFT JOIN VACC16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 15 
          AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
          WHERE v1.VACCINE_ORDER = 11 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )      
) 

-- Creating CTE for HPV (dose 1) where 1 row is per patient AS NUMERATOR. 
--HPV as a SINGLE DOSE 1 >= 12th bday & <= 16th bday OR Dose 1 is null & Dose 2 >=twelfth_bday & <= 16th bday
,HPV AS (
    SELECT 
        v1.PERSON_ID 
        ,v1.VACCINATION_DATE AS hpv_first_date
		,v1.VACCINATION_STATUS as hpv_first_status
        ,v1.AGE_AT_EVENT_OBS as hpv_first_event_age
--HELPER column number of months between first vaccination date and approx twelfth bday. If it's a negative number than the vaccination is early and not valid
       ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.TWELFTH_BDAY)) AS hpv_first_twelfth_bday_mths  
--HELPER COLUMN to check number of months between DOB and first vaccination date is not > 192 months (16 years)
        ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS hpv_first_event_age_mths
        ,v2.VACCINATION_DATE AS hpv_second_date
        ,v2.VACCINATION_STATUS AS hpv_second_status
        ,v2.AGE_AT_EVENT_OBS as hpv_second_event_age
--HELPER column number of months between second vaccination date and approx twelfth bday. If it's a negative number than the vaccination is early and not valid
       ,ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.TWELFTH_BDAY)) AS hpv_second_twelfth_bday_mths
--HELPER COLUMN to check number of months between DOB and second vaccination date is not > 192 months (16 years) 
        ,ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS hpv_second_event_age_mths
        FROM VACC16YRBASE v1
        LEFT JOIN VACC16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ORDER = 17 
        AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
        WHERE v1.VACCINE_ORDER = 16 
        --AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' ) allow for missing first dose
)

-- Creating CTE for MenACWY  (dose 1) where 1 row is per patient AS NUMERATOR 
,MENACWY AS (
    SELECT 
    v1.PERSON_ID
    ,v1.VACCINATION_DATE AS menacwy_first_date
	,v1.VACCINATION_STATUS as menacwy_first_status
    ,v1.AGE_AT_EVENT_OBS as menacwy_first_event_age
--HELPER column number of months between vaccination date and approx 13th bday. If it's a negative number than the vaccination is early and not valid
        ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.THIRTEENTH_BDAY)) AS menacwy_thirteenth_bday_mths   
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months 
         ,ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS menacwy_event_age_mths
    FROM VACC16YRBASE v1
    WHERE v1.VACCINE_ORDER = 19 AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
)


,COMBINED AS (
SELECT distinct
    v.PERSON_ID 
    ,CURRENT_DATE as run_date
    ,'16 Years' as reporting_age
--6-IN-1 ALIGN to HEI current logic - 3 doses anytime before or on 16th bday ie age at event <= 16
    ,CASE WHEN  (s.sixin1_first_status in ('Completed','OutofSchedule') AND s.sixin1_first_event_age <= 16) AND
     (s.sixin1_second_status in ('Completed','OutofSchedule') AND s.sixin1_second_event_age <= 16) AND
 	(s.sixin1_third_status in ('Completed','OutofSchedule') AND s.sixin1_third_event_age <= 16 AND s.sixin1_third_event_age_mths <= 192) THEN 1
	ELSE 0 END AS sixin1_comp_by_16
--------------------------------
--4-IN-1 ALIGN to HEI current logic - 1 doses anytime before or on sixteenth bday - do not restrict by 3years and 4 mths ie age at event <= 16
    ,CASE WHEN (f.fourin1_first_status in ('Completed','OutofSchedule') AND f.fourin1_first_event_age <= 16 AND f.fourin1_event_age_mths <= 192)
  	THEN 1 ELSE 0 END AS fourin1_comp_by_16 
-------------------------------
--HPV as a SINGLE DOSE 1 >= 12th bday (months between vacc date and 12th bday is >= 0) & age at event <= 16 and double check that hpv_first_event_age_mths <= 192
--OR Dose 1 is null & Dose 2 >=twelfth_bday & <= 16th bday
       ,CASE 
        WHEN hp.hpv_first_status in ('Completed','OutofSchedule') AND hp.hpv_first_twelfth_bday_mths >=0  AND hp.hpv_first_event_age <= 16 
        AND hp.hpv_first_event_age_mths <=192 THEN 1
        WHEN hp.hpv_first_date IS NULL AND hp.hpv_second_status in ('Completed','OutofSchedule') AND hp.hpv_second_twelfth_bday_mths >=0  
        AND hp.hpv_second_event_age <= 16 AND hp.hpv_second_event_age_mths <=192 THEN 1
        ELSE 0 END AS hpv_comp_by_16
--------------------------------
--3-IN-1 Green Book teenage TETANUS booster from the age of 13 and 14 (school year 9 or 10)
     ,CASE 
	WHEN t.threein1_first_status in ('Completed','OutofSchedule') AND t.threein1_thirteenth_bday_mths >= 0
    AND t.threein1_first_event_age <= 16 AND t.threein1_event_age_mths <= 192 THEN 1 
    ELSE 0 END AS threein1_comp_by_16 
    -------------------------
    --MMR Doses 1 and 2 Evaluate whether or not MMR (dose 2) has been completed by the sixteenth birthday and after first dose 	and first dose is on or after first b-day
	,CASE WHEN mr.mmr_first_status in ('Completed','OutofSchedule') AND mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0  
    AND mr.mmr_second_date > mr.mmr_first_date AND mr.mmr_second_event_age <=16 AND mr.mmr_second_event_age_mths <=192 THEN 1 ELSE 0 END AS mmr_comp_by_16
---------------------------
 --MENACWY from the age of 13 and 14 (school year 9 or 10)
    ,CASE 
    WHEN ma.menacwy_first_status in ('Completed','OutofSchedule') AND ma.menacwy_thirteenth_bday_mths >= 0
    AND ma.menacwy_first_event_age <= 16 AND ma.menacwy_event_age_mths <= 192 THEN 1 ELSE 0 END AS menacwy_comp_by_16 

FROM VACC16YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join FOURIN1 f using (PERSON_ID)  
left join THREEIN1 t using (PERSON_ID) 
left join MENACWY ma using (PERSON_ID)  
left join MMR mr using (PERSON_ID) 
left join HPV hp using (PERSON_ID)
)

--add back in demographics
select 
c.*
,CASE 
WHEN sixin1_comp_by_16 = 1 AND fourin1_comp_by_16 = 1 AND hpv_comp_by_16 = 1 AND threein1_comp_by_16 = 1 AND menacwy_comp_by_16 = 1 AND mmr_comp_by_16 = 1 	THEN 1 ELSE 0 END AS all_comp_by_16
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
