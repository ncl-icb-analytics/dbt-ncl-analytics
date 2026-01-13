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
--SHINGLES
--SHING_1 turned 65 after 1st sep 2023
WHEN ec.TURN_65_AFTER_SEP_2023 = 'YES' AND ec.VACCINE_ID in ('SHING_1','SHING_2') AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'No' THEN 'Completed' 
WHEN ec.TURN_65_AFTER_SEP_2023 = 'YES' AND ec.VACCINE_ID in ('SHING_1','SHING_2') AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule' 
WHEN ec.TURN_65_AFTER_SEP_2023 = 'YES' AND ec.VACCINE_ID in ('SHING_1','SHING_2') AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ec.TURN_65_AFTER_SEP_2023 = 'YES' AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') THEN 'Not applicable'
--SHING_1B currently aged 70 to 79
WHEN ec.TURN_65_AFTER_SEP_2023 = 'NO' AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'No' THEN 'Completed'  
WHEN ec.TURN_65_AFTER_SEP_2023 = 'NO' AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule' 
WHEN ec.TURN_65_AFTER_SEP_2023 = 'NO' AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ec.TURN_65_AFTER_SEP_2023 = 'NO' AND ec.VACCINE_ID in ('SHING_1','SHING_2') THEN 'Not applicable'
--RSV
--RSV_1B turn 80 after 1st Sep 2024
WHEN ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ec.VACCINE_ID = 'RSV_1B' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'No' THEN 'Completed' 
WHEN ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ec.VACCINE_ID = 'RSV_1B' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule' 
WHEN ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ec.VACCINE_ID = 'RSV_1B' AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ec.TURN_80_AFTER_SEP_2024 = 'YES' AND ec.VACCINE_ID = 'RSV_1' THEN 'Not applicable'
--RSV_1 currently aged 75-79
WHEN ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ec.VACCINE_ID = 'RSV_1' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'No' THEN 'Completed' 
WHEN ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ec.VACCINE_ID = 'RSV_1' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule'
WHEN ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ec.VACCINE_ID = 'RSV_1' AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ec.TURN_80_AFTER_SEP_2024 = 'NO' AND ec.VACCINE_ID = 'RSV_1B' THEN 'Not applicable'
--PPV
WHEN ec.VACCINE_ID = 'PPV_1' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'No' THEN 'Completed' 
WHEN ec.VACCINE_ID = 'PPV_1' AND ve.EVENT_TYPE = 'Administration' AND ve.OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule' 
WHEN ec.VACCINE_ID = 'PPV_1' AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible = 'Yes' THEN 'Overdue'
WHEN ve.EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN ve.EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
WHEN ve.EVENT_DATE IS NULL AND ec.AGE_DAYS_APPROX > ec.maximum_age_days THEN 'No longer eligible' 
ELSE 'Not due yet' END AS VACCINATION_STATUS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT
FROM {{ ref('int_adult_imms_currently_eligible') }} ec
LEFT JOIN {{ ref('int_adult_imms_vaccination_events_current') }} ve ON ec.PERSON_ID = ve.PERSON_ID 
       AND ec.VACCINE_ID = ve.VACCINE_ID    
order by PERSON_ID, VACCINE_ORDER
