--person level vaccination record for adults
{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

SELECT 
ec.PERSON_ID
,ec.BIRTH_DATE_APPROX
,ec.AGE
,ec.TURN_80_AFTER_SEP_2024
,ec.TURN_65_AFTER_SEP_2023
,ec.VACCINE_ORDER
,ec.VACCINE_ID
,ec.VACCINE_NAME
,ec.DOSE_NUMBER
,ec.ELIGIBLE_FROM_DATE 
,ec.ELIGIBLE_TO_DATE
,ec.MAXIMUM_AGE_DAYS
,CASE 
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'No' THEN 'Completed'  
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule'
WHEN ve.EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN ve.EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
WHEN ec.VACCINE_ID = 'RSV_1B' AND ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = 'No' THEN 'No longer eligible' 
WHEN ec.VACCINE_ID = 'RSV_1B' AND ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = 'No' THEN 'No longer eligible' 
WHEN ec.VACCINE_ID = 'RSV_1' AND ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = 'No' THEN 'No longer eligible' 
WHEN ec.VACCINE_ID = 'RSV_1' AND ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = 'No' THEN 'No longer eligible'
WHEN ec.VACCINE_ID = 'RSV_1B' AND ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE THEN 'Overdue' 
WHEN ve.EVENT_DATE IS NULL AND ec.AGE_DAYS_APPROX > ec.maximum_age_days THEN 'No longer eligible' 
ELSE 'Not due yet' END AS VACCINATION_STATUS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT
FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENTLY_ELIGIBLE ec
LEFT JOIN DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_EVENTS_CURRENT ve ON ec.PERSON_ID = ve.PERSON_ID 
       AND ec.VACCINE_ID = ve.VACCINE_ID    
order by PERSON_ID, VACCINE_ORDER
