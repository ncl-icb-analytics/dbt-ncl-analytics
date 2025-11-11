{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT 
ec.PERSON_ID
,ec.BIRTH_DATE_APPROX
,ec.AGE
,ec.BORN_SEP_2022_FLAG
,ec.BORN_JUL_2024_FLAG
,ec.BORN_JAN_2025_FLAG
,ec.FIRST_BDAY
,ec.SECOND_BDAY
,ec.THIRD_bday
,ec.FIFTH_BDAY
,ec.SIXTH_bday
,ec.ELEVENTH_BDAY
,ec.TWELFTH_bday
,ec.THIRTEENTH_bday
,ec.FOURTEENTH_BDAY
,ec.SIXTEENTH_BDAY
,ec.SEVENTEENTH_BDAY
,ec.VACCINE_ORDER
,ec.VACCINE_ID
,ec.VACCINE_NAME
,ec.DOSE_NUMBER
,ec.ELIGIBLE_FROM_DATE 
,ec.ELIGIBLE_TO_DATE
,ec.NEW_VACCINE_APPLICABLE
,ec.MAXIMUM_AGE_DAYS
,CASE 
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'No' THEN 'Completed'  
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule'
WHEN ve.EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN ve.EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
WHEN ve.EVENT_DATE IS NULL and ec.VACCINE_ID in ('6IN1_4','MMRV_1B','MMRV_2B','MMRV_1','MMRV_2','MMRV_1C') AND ec.new_vaccine_applicable = 'Yes' AND DATE(ec.ELIGIBLE_FROM_DATE) < CURRENT_DATE THEN 'Overdue' 
WHEN ve.EVENT_DATE IS NULL and ec.VACCINE_ID in ('6IN1_4','MMRV_1B','MMRV_2B','MMRV_1','MMRV_2','MMRV_1C') AND ec.new_vaccine_applicable = 'No' THEN 'Not applicable'
WHEN ve.EVENT_DATE IS NULL and ec.VACCINE_ID not in ('6IN1_4','MMRV_1B','MMRV_2B','MMRV_1','MMRV_2','MMRV_1C') AND ec.AGE_DAYS_APPROX > ec.maximum_age_days THEN 'No longer eligible' 
WHEN ve.EVENT_DATE IS NULL and ec.VACCINE_ID not in ('6IN1_4','MMRV_1B','MMRV_2B','MMRV_1','MMRV_2','MMRV_1C') AND DATE(ec.ELIGIBLE_FROM_DATE) < CURRENT_DATE AND ec.AGE_DAYS_APPROX < ec.maximum_age_days THEN 'Overdue' 
ELSE 'Not due yet' END AS VACCINATION_STATUS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_currently_eligible') }} ec
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_current') }} ve ON ec.PERSON_ID = ve.PERSON_ID 
       AND ec.VACCINE_ID = ve.VACCINE_ID   
       WHERE ec.AGE < 19

order by PERSON_ID, VACCINE_ORDER