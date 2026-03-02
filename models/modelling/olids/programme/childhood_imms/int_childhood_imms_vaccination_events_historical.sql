{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

--Historical View of Vaccination events. Not linked to Currently eligible population.
WITH POP AS (
select distinct PERSON_ID, BIRTH_DATE_APPROX
FROM {{ ref('int_childhood_imms_historical_population') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_POPULATION
)
,IMMS_CODE_OBS as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine AS VACCINE_NAME,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        clut.schedule_dose as dose_number,
        DATE(o.clinical_effective_date) as EVENT_DATE,
        --o."age_at_event" is from EMIS. It either rounds years up or down
        o.age_at_event AS AGE_AT_EVENT,
        CONCAT(DEM.PERSON_ID, '+', clut.vaccine_id) AS vacc_key
    FROM {{ ref('stg_olids_observation') }} o
    --FROM MODELLING.DBT_STAGING.STG_OLIDS_OBSERVATION o
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = o.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = o.patient_id
    LEFT JOIN POP dem ON pp.PERSON_ID = dem.PERSON_ID
    JOIN {{ ref('int_childhood_imms_code_dose') }} clut on o.mapped_concept_code  = clut.CODE 
    --JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_CODE_DOSE clut on o.mapped_concept_code  = clut.CODE 
    --no future dates
    WHERE o.clinical_effective_date <= CURRENT_DATE
     --imms date must be greater than 1st day of the birth month BIRTH_DATE_APPROX 
   and DATE(o.clinical_effective_date) > DATE_TRUNC('MONTH',dem.BIRTH_DATE_APPROX)
    --look for events across the historical population by age at event in OBS table rather than age of historical means that the number of rows is 1.25 million
    AND o.age_at_event < 19
    and o.mapped_concept_code  = clut.CODE 
         )
--same query using medication orders table rather than observations
,IMMS_CODE_MED as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine AS VACCINE_NAME,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        clut.schedule_dose as dose_number,
        DATE(m.clinical_effective_date) as EVENT_DATE,
        --m."age_at_event" is from EMIS. It either rounds years up or down
        m.age_at_event AS AGE_AT_EVENT,
        CONCAT(DEM.PERSON_ID, '+', clut.vaccine_id) AS vacc_key
    FROM {{ ref('stg_olids_medication_order') }} m
    --FROM MODELLING.DBT_STAGING.STG_OLIDS_MEDICATION_ORDER m
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = m.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = m.patient_id
    LEFT JOIN POP dem ON pp.PERSON_ID = dem.PERSON_ID
    JOIN {{ ref('int_childhood_imms_code_dose') }} clut on m.mapped_concept_code  = clut.CODE 
    --JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_CODE_DOSE clut on m.mapped_concept_code  = clut.CODE 
     --no future dates
    WHERE m.clinical_effective_date <= CURRENT_DATE
     --imms date must be greater than 1st day of the birth month BIRTH_DATE_APPROX 
    and DATE(m.clinical_effective_date) > DATE_TRUNC('MONTH',dem.BIRTH_DATE_APPROX)
    --look for events across the historical population by age at event in OBS table rather than age of historical means that the number of rows is 1.25 million
    AND m.age_at_event < 19
    and m.mapped_concept_code  = clut.CODE 
    )
--UNION OBSERVATIONS AND MEDICATIONS. Only add drug events if they do not already exist as an admin code
,VACCS_COMBINED AS (
select o.*
from IMMS_CODE_OBS o
UNION ALL
select m.*
from IMMS_CODE_MED m
where m.vacc_key NOT IN (SELECT vacc_key FROM IMMS_CODE_OBS)
)        
--Define Vaccination Events by codecluster - do not bother with out of schedule
,IMM_ADM as ( 
     SELECT distinct
        clut.PERSON_ID,
        clut.AGE_AT_EVENT,
	    clut.VACCINE_ORDER,
        clut.VACCINE_ID,
        clut.VACCINE_NAME,
        clut.DOSE_NUMBER,
        clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid LIKE '%_ADM' THEN 'Administration'
            WHEN clut.codeclusterid LIKE '%_DRUG' THEN 'Administration_drug'
            WHEN clut.codeclusterid LIKE '%_CONTRA' THEN 'Contraindicated'
            WHEN clut.codeclusterid LIKE '%_DEC' THEN 'Declined'
            ELSE NULL
        END AS EVENT_TYPE
        FROM VACCS_COMBINED clut 
       )
--IDENTIFY ROWS WHERE DECLINED OR CONTRAINDICATED AND ADMINSTRATION EVENTS ARE ON THE SAME DATE. PRIORITISE ADMINISTRATION
,IMM_ADM_CLUSTER as (
SELECT 
    PERSON_ID,
   AGE_AT_EVENT,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    EVENT_DATE,
    EVENT_TYPE,
--Administration wins on dates where both appear; a single Declined/Contra remains
   ROW_NUMBER() OVER (
        PARTITION BY PERSON_ID, VACCINE_ID, EVENT_DATE
        ORDER BY
            CASE 
                WHEN event_type LIKE 'Admin%' THEN 1
                ELSE 2
            END
    ) AS r
FROM IMM_ADM
QUALIFY r = 1
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
	ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num
   --    COUNT(*) OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE) AS TOTAL_EVENTS (no longer required)
    FROM IMM_ADM_CLUSTER   
      ) 
--SELECT FINAL VACCINATIONS DATASET DE-DUPLICATION BY EVENT_DATE and DOSE
SELECT 
	PERSON_ID,
	AGE_AT_EVENT,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE
	FROM IMM_ADM_RANKED
WHERE 
--deduplicate where codes are non dose specific 
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2)  
OR (dose_number = 3 AND row_num = 3) 
OR (dose_number = 4 AND row_num = 4) 
-- Include single-entry cases for dose specific MenB and Rotavirus and HPV. No longer required all vaccines are now non-dose-specific
--OR (VACCINE_ID in ('ROTA_2','MENB_2','MENB_2B','MENB_3', 'HPV_2') AND total_events = 1) 