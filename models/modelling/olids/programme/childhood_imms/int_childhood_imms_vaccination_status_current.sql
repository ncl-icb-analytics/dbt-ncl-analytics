{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT 
ec.PERSON_ID
,ec.BIRTH_DATE_APPROX
,ec.DOB_EOM
,ec.AGE
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
,CASE 
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'No' THEN 'Completed'  
WHEN ve.EVENT_TYPE = 'Administration' AND OUT_OF_SCHEDULE = 'Yes' THEN 'OutofSchedule'
WHEN ve.EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN ve.EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
WHEN ve.EVENT_DATE IS NULL  and DATE(ec.ELIGIBLE_FROM_DATE) < CURRENT_DATE THEN 'Overdue' 
ELSE 'Not due yet'
END as VACCINATION_STATUS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT_OBS
FROM {{ ref('int_childhood_imms_currently_eligible') }} ec
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_current') }} ve ON ec.PERSON_ID = ve.PERSON_ID 
       AND ec.VACCINE_ID = ve.VACCINE_ID   
       WHERE ec.AGE < 19

order by PERSON_ID, VACCINE_ORDER