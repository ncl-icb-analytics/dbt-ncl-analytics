{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

--FIND all vaccination events by joining the mapped concept codes in the observation table.
WITH IMMS_CODE_OBS as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        DATE(o.clinical_effective_date) as EVENT_DATE,
        --o."age_at_event" is from EMIS. It either rounds years up or down
        o.age_at_event AS AGE_AT_EVENT_OBS,
        clut.administered_cluster_id, 
        clut.drug_cluster_id,
        clut.declined_cluster_id,
        clut.contraindicated_cluster_id 
   FROM {{ ref('stg_olids_observation') }} o
   --FROM MODELLING.DBT_STAGING.STG_OLIDS_OBSERVATION o
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = o.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = o.patient_id
    LEFT JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
    --LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS dem ON pp.PERSON_ID = dem.PERSON_ID
    JOIN {{ ref('int_childhood_imms_code_dose') }} clut on o.mapped_concept_code  = clut.CODE
    -- JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_CODE_DOSE clut on o.mapped_concept_code  = = clut.CODE 
    WHERE o.clinical_effective_date <= CURRENT_DATE
    AND dem.age < 19
    and o.mapped_concept_code  = clut.CODE
    )
--FIND all vaccination events by joining the mapped concept codes in the medication orders table.
,IMMS_CODE_MED as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        DATE(m.clinical_effective_date) as EVENT_DATE,
        --m."age_at_event" is from EMIS. It either rounds years up or down
        m.age_at_event AS AGE_AT_EVENT_OBS,
        clut.administered_cluster_id, 
        clut.drug_cluster_id,
        clut.declined_cluster_id,
        clut.contraindicated_cluster_id
    FROM {{ ref('stg_olids_medication_order') }} m
    --FROM MODELLING.DBT_STAGING.STG_OLIDS_MEDICATION_ORDER m
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = m.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = m.patient_id
    LEFT JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
    --LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS dem ON pp.PERSON_ID = dem.PERSON_ID
    JOIN {{ ref('int_childhood_imms_code_dose') }} clut on m.mapped_concept_code  = clut.CODE
    WHERE m.clinical_effective_date <= CURRENT_DATE
    AND dem.age < 19
    and m.mapped_concept_code  = clut.CODE
)
--UNION OBSERVATIONS AND MEDICATIONS. Only add drug events if they do not already exist as an admin code
,VACCS_COMBINED AS (
select o.*
from IMMS_CODE_OBS o
UNION ALL
select m.*
from IMMS_CODE_MED m
WHERE (m.PERSON_ID, m.VACCINE_ID) NOT IN (
            SELECT PERSON_ID, VACCINE_ID FROM IMMS_CODE_OBS)
)
--MATCH RECORDED IMMS EVENTS TO ELIGIBLE POPULATION AND DEFINE 'OUT OF SCHEDULE'
,IMM_ADM_ELIG as (  
     SELECT distinct
        el.PERSON_ID,
        el.BIRTH_DATE_APPROX,
        el.BORN_SEP_2022_FLAG,
        el.BORN_JUL_2024_FLAG,
        el.BORN_JAN_2025_FLAG,
        el.AGE_DAYS_APPROX,
        clut.AGE_AT_EVENT_OBS,
	    clut.VACCINE_ORDER,
        el.VACCINE_ID,
        el.VACCINE_NAME,
        el.DOSE_NUMBER,
        el.ELIGIBLE_FROM_DATE,
        el.ELIGIBLE_TO_DATE,
        el.MAXIMUM_AGE_DAYS,
       clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid = clut.administered_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.drug_cluster_id THEN 'Administration_drug'
            WHEN clut.codeclusterid = clut.Contraindicated_Cluster_ID THEN 'Contraindicated'
            WHEN clut.codeclusterid = clut.Declined_Cluster_ID THEN 'Declined'
            ELSE NULL
        END AS EVENT_TYPE,
         -- Determine if the event was out of schedule for any of the events not just (clut.administered_cluster_id,clut.drug_cluster_id )
        CASE 
            WHEN datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) > el.ELIGIBLE_AGE_TO_DAYS + 15 THEN 'Yes' 
            WHEN datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) < el.ELIGIBLE_AGE_FROM_DAYS - 15 THEN 'Yes' 
           ELSE 'No' 
        END AS OUT_OF_SCHEDULE      
    FROM {{ ref('int_childhood_imms_currently_eligible') }} el 
    INNER JOIN VACCS_COMBINED clut on clut.PERSON_ID = el.PERSON_ID
    AND el.DOSE_NUMBER = clut.DOSE_MATCH 
    and el.VACCINE_NAME = clut.VACCINE
    and el.VACCINE_ID = clut.VACCINE_ID
       --imms date must be greater than 1st day of the birth month BIRTH_DATE_APPROX
    and clut.EVENT_DATE > DATE_TRUNC('MONTH',el.BIRTH_DATE_APPROX)
    AND el.age < 19
   )
--IDENTIFY ROWS WHERE DECLINED OR CONTRAINDICATED AND ADMINSTRATION EVENTS ARE ON THE SAME DATE. PRIORITISE ADMINISTRATION
,IMM_ADM_CLUSTER as (
SELECT 
    PERSON_ID,
    BIRTH_DATE_APPROX,
    BORN_SEP_2022_FLAG,
    BORN_JUL_2024_FLAG,
    BORN_JAN_2025_FLAG,
    AGE_DAYS_APPROX,
    AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    ELIGIBLE_FROM_DATE,
    ELIGIBLE_TO_DATE,
    MAXIMUM_AGE_DAYS,
    EVENT_DATE,
    EVENT_TYPE,
    OUT_OF_SCHEDULE,
     --Administration wins on dates where both appear; a single Declined/Contra remains
   ROW_NUMBER() OVER (
        PARTITION BY PERSON_ID, VACCINE_ID, EVENT_DATE
        ORDER BY
            CASE 
                WHEN event_type LIKE 'Admin%' THEN 1
                ELSE 2
            END
    ) AS r
FROM IMM_ADM_ELIG
QUALIFY r = 1
    ) 
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES
,IMM_ADM_RANKED as (
SELECT 
	PERSON_ID,
    BIRTH_DATE_APPROX,
    BORN_SEP_2022_FLAG,
    BORN_JUL_2024_FLAG,
    BORN_JAN_2025_FLAG,
    AGE_DAYS_APPROX,
	AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    ELIGIBLE_FROM_DATE,
    ELIGIBLE_TO_DATE,
    MAXIMUM_AGE_DAYS,
    EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE,
       ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num
    --COUNT(*) OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE) AS TOTAL_EVENTS (no longer required)
    FROM IMM_ADM_CLUSTER   
      ) 
--SELECT FINAL VACCINATIONS DATASET DE-DUPLICATION BY EVENT_DATE and DOSE 
,FINAL_VACCS as (
 SELECT 
	PERSON_ID,
    BIRTH_DATE_APPROX,
    BORN_SEP_2022_FLAG,
    BORN_JUL_2024_FLAG,
    BORN_JAN_2025_FLAG,
    age_days_approx,
	AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    ELIGIBLE_FROM_DATE,
    ELIGIBLE_TO_DATE,
    MAXIMUM_AGE_DAYS,
    EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE
	FROM IMM_ADM_RANKED
WHERE 
--deduplicate where codes are non dose specific 
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2)  
OR (dose_number = 3 AND row_num = 3) 
OR (dose_number = 4 AND row_num = 4) 
-- Include single-entry cases for dose specific MenB and Rotavirus and HPV NO LONGER REQUIRED AS ALL DOSES ARE NOW NON-DOSE SPECIFIC
--OR (VACCINE_ID in ('ROTA_2','MENB_2','MENB_2B','MENB_3', 'HPV_2') AND total_events = 1)
)
--ADD VACCINATION STATUS FOR EVENTS
select *
,CASE 
--new vaccines that don't apply for those for born on or after 1st Jan 2025
WHEN VACCINE_ID in ('PCV_1','MENB_2','MMR_1','MMRV_1B','MMRV_1C','MMR_2','MMRV_2B','HIBMENC_1')  AND (BORN_JAN_2025_FLAG = 'Yes')  THEN 'Not applicable'
--new vaccines that don't apply for those for born on or after 1st July 2024
WHEN VACCINE_ID in ('PCV_1','MENB_2','MMR_1','MMRV_1','MMRV_1C','MMR_2','MMRV_2','HIBMENC_1')  AND (BORN_JUL_2024_FLAG = 'Yes')  THEN 'Not applicable'
--new vaccines that don't apply for those for born on or after 22nd September 2022
WHEN VACCINE_ID in ('PCV_1B','MENB_2B','MMRV_1','MMRV_1B','MMR_2','MMRV_2','MMRV_2B','6IN1_4')  AND (BORN_SEP_2022_FLAG = 'Yes')  THEN 'Not applicable'
--new vaccines that don't apply for those for born before 22nd September 2022
WHEN VACCINE_ID in ('PCV_1B','MENB_2B','MMRV_1','MMRV_1B','MMRV_1C','MMRV_2','MMRV_2B','6IN1_4')  AND BIRTH_DATE_APPROX < '2022-09-01'  THEN 'Not applicable'
WHEN EVENT_DATE IS NULL AND ELIGIBLE_FROM_DATE >= CURRENT_DATE() THEN 'Not due yet'
WHEN EVENT_DATE IS NULL AND ELIGIBLE_FROM_DATE < CURRENT_DATE() AND AGE_DAYS_APPROX < maximum_age_days THEN 'Overdue'
WHEN EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'No' THEN 'Completed'  
WHEN EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule'
WHEN EVENT_DATE IS NULL AND AGE_DAYS_APPROX > maximum_age_days THEN 'No longer eligible'
WHEN EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
END as VACCINATION_STATUS
from FINAL_VACCS