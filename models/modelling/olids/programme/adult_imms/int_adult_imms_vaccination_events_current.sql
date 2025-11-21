{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

WITH IMMS_CODE_OBS as (
--deduplicate across vaccine_ids and clusters - order by select the latest
    SELECT
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        DATE(o.clinical_effective_date) AS EVENT_DATE,
        o.age_at_event,
         clut.administered_cluster_id, 
        clut.drug_cluster_id,
        clut.declined_cluster_id,
        clut.contraindicated_cluster_id,  
        --choose latest code grouped by person, vaccine_id and cluster
        ROW_NUMBER() OVER (PARTITION BY dem.PERSON_ID, clut.vaccine_id, clut.CODECLUSTERID ORDER BY DATE(o.clinical_effective_date) DESC) AS rn
    FROM {{ ref('stg_olids_observation') }} o
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = o.patient_id
    LEFT JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
         -- Join mapped_concept_code to the IMMS_CODE_DOSEMATCH making sure clut.code is VARCHAR (currently is number)
    JOIN {{ ref('int_adult_imms_code_dose') }} clut on o.mapped_concept_code  = CAST(clut.CODE AS VARCHAR) 
        WHERE o.clinical_effective_date <= CURRENT_DATE
        and o.mapped_concept_code  = clut.CODE
 --choose latest code grouped by person, vaccine_id and cluster
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dem.PERSON_ID, clut.vaccine_id, clut.CODECLUSTERID ORDER BY DATE(o.clinical_effective_date) DESC) = 1
)
--Define Vaccination Events look for ADMIN CODES by matching IMMS_CODE_OBS to CURRENTLY ELIGIBLE 
,IMM_ADM as ( 
     SELECT distinct
        el.PERSON_ID,
        clut.AGE_AT_EVENT,
	    clut.VACCINE_ORDER,
        el.VACCINE_ID,
        el.VACCINE_NAME,
        el.DOSE_NUMBER,
        clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid = clut.administered_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.drug_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.Contraindicated_Cluster_ID THEN 'Contraindicated'
            WHEN clut.codeclusterid = clut.Declined_Cluster_ID THEN 'Declined'
            ELSE 'Other'
        END AS EVENT_TYPE,
         -- Determine if the event was out of schedule (only for Administration)
        CASE 
            WHEN clut.codeclusterid = clut.administered_cluster_id 
            AND datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) > el.ELIGIBLE_AGE_TO_DAYS + 15 THEN 'Yes' 
            WHEN clut.codeclusterid = clut.administered_cluster_id 
            AND datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) < el.ELIGIBLE_AGE_FROM_DAYS - 15 THEN 'Yes' 
            ELSE 'No' 
        END AS OUT_OF_SCHEDULE    
    FROM {{ ref('int_adult_imms_currently_eligible') }} el 
    INNER JOIN IMMS_CODE_OBS clut on clut.PERSON_ID = el.PERSON_ID
    AND el.DOSE_NUMBER = clut.DOSE_MATCH 
    and el.VACCINE_NAME = clut.VACCINE
    and el.VACCINE_ID = clut.VACCINE_ID   
   )
 ,IMM_ADM_CLUSTER as (
-- --IDENTIFY ROWS WITH DECLINED AS WELL AS ADMINSTERED AND CHOOSE ADMINSTERED OVER DECLINED
SELECT 
    PERSON_ID,
    AGE_AT_EVENT,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    EVENT_DATE,
    EVENT_TYPE,
    OUT_OF_SCHEDULE,
    -- RANK() OVER (
    --     PARTITION BY PERSON_ID, VACCINE_ID 
    --     ORDER BY CASE 
    --         WHEN EVENT_TYPE = 'Declined' THEN 0
    --         WHEN EVENT_TYPE = 'Contraindicated' THEN 0
    --         WHEN EVENT_TYPE = 'Administration' THEN 1
    --     END DESC
    -- ) AS r
FROM IMM_ADM
QUALIFY RANK() OVER (PARTITION BY PERSON_ID, VACCINE_ID 
        ORDER BY CASE 
            WHEN EVENT_TYPE = 'Declined' THEN 0
            WHEN EVENT_TYPE = 'Contraindicated' THEN 0
            WHEN EVENT_TYPE = 'Administration' THEN 1
        END DESC) =1
)
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES
,IMM_ADM_RANKED as (
	SELECT 
	PERSON_ID,
	AGE_AT_EVENT,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE,
       ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num,
       COUNT(*) OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE) AS TOTAL_EVENTS
    FROM IMM_ADM_CLUSTER     
      ) 
--SELECT FINAL DATASET 

 SELECT 
	PERSON_ID,
	AGE_AT_EVENT,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE
	FROM IMM_ADM_RANKED
WHERE 
--deduplicate where codes are non dose specific RSV, PPV, SHINGLES
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2)
--to capture SHINGlES dose 2 where only one code exists
OR (VACCINE_ID in ('SHING_2','SHING_2B') AND total_events = 1)