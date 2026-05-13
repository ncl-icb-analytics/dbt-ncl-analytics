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
,ec.IS_CARE_HOME_RESIDENT
,ec.TURN_65_AFTER_SEP_2023
,ec.TURN_75_AFTER_SEP_2024
,ec.TURN_80_AFTER_SEP_2024
,ec.VACCINE_ORDER
,ec.VACCINE_ID
,ec.VACCINE_NAME
,ec.DOSE_NUMBER
,ec.ELIGIBLE_FROM_DATE 
,ec.ELIGIBLE_TO_DATE
,ec.MAXIMUM_AGE_DAYS
--Add in additional logic for where vaccination events are missing.
,CASE 
--SHINGLES---------------------------------------------------------------
--SHING_1 turned 65 after 1st sep 2023
WHEN ec.TURN_65_AFTER_SEP_2023 AND ec.VACCINE_ID in ('SHING_1','SHING_2') AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE <= CURRENT_DATE AND ec.currently_eligible THEN 'Overdue'
WHEN ec.TURN_65_AFTER_SEP_2023 AND ec.VACCINE_ID in ('SHING_1','SHING_2') AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = FALSE AND ec.AGE_DAYS_APPROX <= ec.maximum_age_days THEN 'Not due yet'
WHEN ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ec.TURN_65_AFTER_SEP_2023 AND ve.EVENT_DATE IS NULL THEN 'Not applicable'
--SHING 1B TURNED 65 BEFORE 1st Sep 2023
WHEN ec.TURN_65_AFTER_SEP_2023 = FALSE AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE <= CURRENT_DATE AND ec.currently_eligible THEN 'Overdue'
WHEN ec.TURN_65_AFTER_SEP_2023 = FALSE AND ec.VACCINE_ID in ('SHING_1B','SHING_2B') AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = FALSE AND ec.AGE_DAYS_APPROX <= ec.maximum_age_days THEN 'Not due yet'
WHEN ec.VACCINE_ID in ('SHING_1','SHING_2')  AND ec.TURN_65_AFTER_SEP_2023 = FALSE  AND ve.EVENT_DATE IS NULL THEN 'Not applicable'
--RSV------------------------------------------------------------
--RSV_1 THOSE AGED 75 or OLDER starting 1st April 2026
WHEN ec.VACCINE_ID in ('RSV_1')  AND ec.IS_CARE_HOME_RESIDENT = FALSE AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE <= CURRENT_DATE AND ec.currently_eligible THEN 'Overdue'
WHEN ec.VACCINE_ID in ('RSV_1')  AND ec.IS_CARE_HOME_RESIDENT = FALSE AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = FALSE AND ec.AGE_DAYS_APPROX <= ec.maximum_age_days THEN 'Not due yet'
WHEN ec.VACCINE_ID in ('RSV_1')  AND ec.IS_CARE_HOME_RESIDENT AND ve.EVENT_DATE IS NULL THEN 'Not applicable'
--RSV_1B THOSE IN A CARE HOME FOR OLDER PEOPLE 
WHEN ec.VACCINE_ID in ('RSV_1B')  AND ec.IS_CARE_HOME_RESIDENT AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible THEN 'Overdue'
WHEN ec.VACCINE_ID in ('RSV_1B')  AND ec.IS_CARE_HOME_RESIDENT AND ve.EVENT_DATE IS NULL AND ec.currently_eligible = FALSE AND ec.AGE_DAYS_APPROX <= ec.maximum_age_days THEN 'Not due yet'
WHEN ec.VACCINE_ID in ('RSV_1B')  AND ec.IS_CARE_HOME_RESIDENT = FALSE AND ve.EVENT_DATE IS NULL THEN 'Not applicable'
---PPV------------------------------------------------------------------------
WHEN ec.VACCINE_ID = 'PPV_1' AND ve.EVENT_DATE IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE AND ec.currently_eligible THEN 'Overdue'
WHEN ec.VACCINE_ID = 'PPV_1' AND ve.EVENT_DATE IS NULL  AND ec.currently_eligible = FALSE AND ec.AGE_DAYS_APPROX <= ec.maximum_age_days THEN 'Not due yet'
WHEN ve.EVENT_DATE IS NULL AND ec.AGE_DAYS_APPROX > ec.maximum_age_days THEN 'No longer eligible' 
ELSE ve.vaccination_status END AS VACCINATION_STATUS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT
FROM {{ ref('int_adult_imms_currently_eligible') }} ec
LEFT JOIN {{ ref('int_adult_imms_vaccination_events_current') }} ve ON ec.PERSON_ID = ve.PERSON_ID 
       AND ec.VACCINE_ID = ve.VACCINE_ID    

