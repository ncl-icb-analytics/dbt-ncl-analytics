{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

with VACC1YRBASE as (
SELECT DISTINCT
PERSON_ID
,BIRTH_DATE_APPROX
,BORN_JUL_2024_FLAG
,BORN_JAN_2025_FLAG
,FIRST_BDAY
,SECOND_BDAY
,VACCINE_ORDER
,VACCINE_ID
,VACCINATION_STATUS
,VACCINATION_DATE
,AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_vaccination_status_current') }}
--Children that are currently aged 1 (based on approx dob) for base population selected by age only, not relevant vaccinations
WHERE AGE = 1
--Replace AGE = 1 for more accurate Age Bucket which is created from actual DOB
--WHERE age_bucket = 'Age 1-2'

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
    --HELPER COLUMN to check number of months between DOB and 3rd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v3.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM VACC1YRBASE v1
    LEFT JOIN VACC1YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue')
    LEFT JOIN VACC1YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.VACCINATION_STATUS not in ('Declined', 'Contraindicated' ,'Overdue')
    WHERE v1.VACCINE_ID = '6IN1_1' AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )  
)
 -- Creating CTE for Rotavirus (dose 1 and 2) where 1 row is per patient AS NUMERATOR
,ROTA AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS rota_first_date, 
        v1.VACCINATION_STATUS AS rota_first_status,
        v1.AGE_AT_EVENT_OBS as  rota_first_event_age,
        v2.VACCINATION_DATE AS rota_second_date,
        v2.VACCINATION_STATUS AS rota_second_status,
        v2.AGE_AT_EVENT_OBS as  rota_second_event_age,
        --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS rota_second_event_age_mths
    FROM VACC1YRBASE v1
    LEFT JOIN VACC1YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'ROTA_2' AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
    WHERE v1.VACCINE_ID = 'ROTA_1' and v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
-- Creating CTE for MenB (dose 1 and 2) where 1 row is per patient AS NUMERATOR
,MENB AS (
    SELECT 
         v1.PERSON_ID, 
         v1.VACCINATION_STATUS AS menb_first_status,
         v1.VACCINATION_DATE AS menb_first_date, 
          v1.AGE_AT_EVENT_OBS as menb_first_event_age,
         v2.VACCINATION_STATUS AS menb_second_status,
         v2.VACCINATION_DATE AS menb_second_date,
         v2.AGE_AT_EVENT_OBS as menb_second_event_age,
    --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
    ROUND(MONTHS_BETWEEN(v2.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS menb_second_event_age_mths
    FROM VACC1YRBASE v1
    LEFT JOIN VACC1YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MENB_2' AND v2.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
    WHERE v1.VACCINE_ID = 'MENB_1' AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
)
-- Creating CTE for PCV (dose 1) where 1 row is per patient AS NUMERATOR
,PCV AS (
    SELECT 
        v1.PERSON_ID, 
        v1.VACCINATION_DATE AS pcv_first_date,
        v1.VACCINATION_STATUS as pcv_first_status,
        v1.AGE_AT_EVENT_OBS as pcv_first_event_age,
    --HELPER COLUMN to check number of months between DOB and 2nd vaccination date to check not 12 months
        ROUND(MONTHS_BETWEEN(v1.VACCINATION_DATE, v1.BIRTH_DATE_APPROX)) AS pcv_first_event_age_mths
         FROM VACC1YRBASE v1
        WHERE v1.VACCINE_ID = 'PCV_1' AND v1.VACCINATION_STATUS not in ('Declined', 'Contraindicated','Overdue' )
) 
,COMBINED AS (
SELECT distinct
    v.PERSON_ID, 
    CURRENT_DATE as run_date,
    '1 Year' as reporting_age,
   --6-IN-1 ALIGN to HEI current logic - 3 doses anytime before or on first bday
      --all sixin1 age at event <=1 and check that third sixin1 age at event in months <=12
    CASE 
    WHEN  (s.sixin1_first_status in ('Completed','OutofSchedule') AND s.sixin1_first_event_age <= 1) AND
     (s.sixin1_second_status in ('Completed','OutofSchedule') AND s.sixin1_second_event_age <= 1) AND
 	(s.sixin1_third_status in ('Completed','OutofSchedule') AND s.sixin1_third_event_age <= 1 AND s.sixin1_third_event_age_mths <= 12) THEN 1
		 ELSE 0 end as sixin1_comp_by_1,
    --ROTAVIRUS ALIGN to HEI current logic - 2 doses anytime before or on first bday ie age at event <=1 and check that second Rota age at event in months <=12
      CASE WHEN (r.rota_first_status in ('Completed','OutofSchedule') AND r.rota_first_event_age <= 1) AND
       (r.rota_second_status in ('Completed','OutofSchedule') AND r.rota_second_event_age <= 1 AND r.rota_second_event_age_mths <= 12)  THEN 1
		 ELSE 0 end as rota_comp_by_1,
   --MEN B ALIGN to HEI current logic - 2 doses anytime before or on first bday ie both MenB age at event <=1 and check that second Menb age at event in months <=12
      CASE WHEN (m.menb_first_status in ('Completed','OutofSchedule') AND m.menb_first_event_age <= 1) AND
       (m.menb_second_status in ('Completed','OutofSchedule') AND m.menb_second_event_age <= 1 AND m.menb_second_event_age_mths <= 12)  THEN 1
		 else 0 end as menb_comp_by_1,
       --PCV NOT ALIGN to HEI current logic - 1 dose anytime before or on first bday - not restricted to after or on 12 weeks. This will be flagged as out of schedule instead
    CASE WHEN (p.pcv_first_status in ('Completed','OutofSchedule') AND p.pcv_first_event_age <= 1 AND p.pcv_first_event_age_mths <=12 ) THEN 1
        ELSE 0 END AS pcv_comp_by_1   
FROM VACC1YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join ROTA r using (PERSON_ID) 
left join MENB m using (PERSON_ID)
left join PCV p using (PERSON_ID) 
)  
--add back in demographics
select 
c.*
,CASE 
WHEN sixin1_comp_by_1 = 1 AND rota_comp_by_1 = 1 AND menb_comp_by_1 = 1 AND pcv_comp_by_1 = 1 THEN 1
ELSE 0 END AS all_comp_by_1
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