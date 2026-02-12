{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT 
ec.PERSON_ID
,ec.BIRTH_DATE_APPROX
,ec.AGE
,ec.AGE_DAYS_APPROX
,CONCAT(
        FLOOR(ec.AGE_DAYS_APPROX / 365), 'yrs, ',
        FLOOR(MOD(ec.AGE_DAYS_APPROX, 365) / 30), 'mths, ',
        MOD(MOD(ec.AGE_DAYS_APPROX, 365), 30), 'days'
     ) AS formatted_age
,ec.BORN_SEP_2022_FLAG
,ec.BORN_JUL_2024_FLAG
,ec.BORN_JAN_2025_FLAG
,ec.FIRST_BDAY
,ec.SECOND_BDAY
,ec.TWELFTH_BDAY
,ec.THIRTEENTH_BDAY
,ec.VACCINE_ORDER
,ec.VACCINE_ID
,ec.VACCINE_NAME
,ec.DOSE_NUMBER
,ec.ELIGIBLE_FROM_DATE 
,ec.ELIGIBLE_TO_DATE
,ec.NEW_VACCINE_APPLICABLE
,ec.MAXIMUM_AGE_DAYS
,ve.EVENT_DATE as VACCINATION_DATE
,ve.OUT_OF_SCHEDULE
,ve.AGE_AT_EVENT_OBS
--ADJUST VACCINATION STATUS WHERE VACCINATION EVENTS ARE MISSING
,CASE
--new vaccines that don't apply for those for born on or after 1st Jan 2025
WHEN ve.VACCINATION_STATUS IS NULL AND ec.BORN_JAN_2025_FLAG = 'Yes' AND ec.VACCINE_ID in ('PCV_1','MENB_2','MMR_1','MMRV_1B','MMRV_1C','MMR_2','MMRV_2B','HIBMENC_1') THEN 'Not applicable'
--new vaccines that don't apply for those for born on or after 1st July 2024
WHEN ve.VACCINATION_STATUS IS NULL AND ec.BORN_JUL_2024_FLAG = 'Yes' AND ec.VACCINE_ID in ('PCV_1','MENB_2','MMR_1','MMRV_1','MMRV_1C','MMR_2','MMRV_2','HIBMENC_1') THEN 'Not applicable'
--new vaccines that don't apply for those for born on or after 22nd September 2022
WHEN ve.VACCINATION_STATUS IS NULL AND ec.BORN_SEP_2022_FLAG = 'Yes' AND ec.VACCINE_ID in ('PCV_1B','MENB_2B','MMRV_1','MMRV_1B','MMR_2','MMRV_2','MMRV_2B','6IN1_4') THEN 'Not applicable'
--new vaccines that don't apply for those for born before 22nd September 2022
WHEN ve.VACCINATION_STATUS IS NULL AND ec.BIRTH_DATE_APPROX < '2022-09-01' AND ec.VACCINE_ID in ('PCV_1B','MENB_2B','MMRV_1','MMRV_1B','MMRV_1C','MMRV_2','MMRV_2B','6IN1_4') THEN 'Not applicable'
--apply final rules for those with no vaccination events observed
WHEN ve.VACCINATION_STATUS IS NULL AND ec.ELIGIBLE_FROM_DATE >= CURRENT_DATE() THEN 'Not due yet'
WHEN ve.VACCINATION_STATUS IS NULL AND ec.ELIGIBLE_FROM_DATE < CURRENT_DATE() AND ec.AGE_DAYS_APPROX < ec.MAXIMUM_AGE_DAYS THEN 'Overdue'
WHEN ve.VACCINATION_STATUS IS NULL AND ec.AGE_DAYS_APPROX > ec.MAXIMUM_AGE_DAYS THEN 'No longer eligible'
ELSE ve.VACCINATION_STATUS END AS VACCINATION_STATUS
FROM {{ ref('int_childhood_imms_currently_eligible') }} ec
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_CURRENTLY_ELIGIBLE ec
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_current') }} ve ON ec.PERSON_ID = ve.PERSON_ID 
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_VACCINATION_EVENTS_CURRENT ve ON ec.PERSON_ID = ve.PERSON_ID
       AND ec.VACCINE_ID = ve.VACCINE_ID   
       WHERE ec.AGE < 19

order by PERSON_ID, VACCINE_ORDER