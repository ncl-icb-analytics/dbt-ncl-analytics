{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH IMMS_CODE_OBS as (
SELECT DISTINCT 
        dem.PERSON_ID,
        dem.birth_date_approx,
        CASE WHEN dem.BIRTH_DATE_APPROX >= '2024-07-01' THEN 'Yes'
        ELSE 'No' END AS BORN_JUL_2024_FLAG,
        clut.VACCINE_ORDER,
        clut.vaccine AS VACCINE_NAME,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.dose_match,
        clut.schedule_dose as dose_number,
        DATE(o.clinical_effective_date) as EVENT_DATE,
        --o."age_at_event" is from EMIS. It either rounds years up or down
        o.age_at_event AS AGE_AT_EVENT_OBS,
         clut.administered_cluster_id, 
        clut.drug_cluster_id,
        clut.declined_cluster_id,
        clut.contraindicated_cluster_id 
    FROM {{ ref('stg_olids_observation') }} o
     LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = o.patient_id
    LEFT JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
     -- Join mapped_concept_code to the IMMS_CODE_DOSEMATCH making sure clut.code is VARCHAR (currently is number)
    JOIN {{ ref('int_childhood_imms_code_dose') }} clut on o.mapped_concept_code  = CAST(clut.CODE AS VARCHAR) 
    WHERE o.clinical_effective_date <= CURRENT_DATE
    --look for events across the historical population by age at event in OBS table rather than age of historical means that the number of rows is 1.25 million
    AND o.age_at_event < 19
    and o.mapped_concept_code  = clut.CODE 
  
       )
--Define Vaccination Events by codecluster - do not bother with out of schedule
,IMM_ADM as ( 
     SELECT distinct
        clut.PERSON_ID,
        clut.AGE_AT_EVENT_OBS,
	    clut.VACCINE_ORDER,
        clut.VACCINE_ID,
        clut.VACCINE_NAME,
        clut.DOSE_NUMBER,
        clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid = clut.administered_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.drug_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.Contraindicated_Cluster_ID THEN 'Contraindicated'
            WHEN clut.codeclusterid = clut.Declined_Cluster_ID THEN 'Declined'
            ELSE 'Other'
        END AS EVENT_TYPE
        FROM IMMS_CODE_OBS clut 
          --imms date must be greater than 1st day of the birth month BIRTH_DATE_APPROX 
    WHERE clut.EVENT_DATE > DATE_TRUNC('MONTH',clut.BIRTH_DATE_APPROX)
       )
  --IDENTIFY DUPLICATE ROWS WHERE DECLINED OR CONTRAINDICATED AND ADMINSTRATION ON THE SAME DATE
,IMM_ADM_CLUSTER as (
SELECT 
    PERSON_ID,
    AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    EVENT_DATE,
    EVENT_TYPE,
    -- OUT_OF_SCHEDULE,
    RANK() OVER (
        PARTITION BY PERSON_ID, VACCINE_ID 
        ORDER BY CASE 
            WHEN EVENT_TYPE = 'Declined' THEN 0
            WHEN EVENT_TYPE = 'Contraindicated' THEN 0
            WHEN EVENT_TYPE = 'Administration' THEN 1
        END DESC
    ) AS r
FROM IMM_ADM
QUALIFY r = 1
    ) 
  
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES
,IMM_ADM_RANKED as (
SELECT 
	PERSON_ID,
	AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE,
	ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num,
       COUNT(*) OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE) AS TOTAL_EVENTS
    FROM IMM_ADM_CLUSTER   
      ) 

--SELECT FINAL DATASET 
 SELECT 
	PERSON_ID,
	AGE_AT_EVENT_OBS,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE
	FROM IMM_ADM_RANKED
WHERE 
--deduplicate where codes are non dose specific PCV, 6-in-1, MMR
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2)  
OR (dose_number = 3 AND row_num = 3) 
OR (dose_number = 4 AND row_num = 4) 
-- Include single-entry cases for dose specific MenB and Rotavirus and HPV
OR (VACCINE_ID in ('ROTA_2','MENB_2','MENB_2B','MENB_3', 'HPV_2') AND total_events = 1)