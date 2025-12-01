{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 75+ who are eligible who have received RSV vaccine. Latest vaccination recorded
--people eligible either turned 80 after Sept 2024 (RSV_1B) or are currently 75-79 for the Catch Up programme (RSV_1)
SELECT 
         PERSON_ID 
        ,AGE AS CURRENT_AGE
        ,TURN_80_AFTER_SEP_2024
        ,VACCINE_ID as VACCINE_ID_FIRST
        ,VACCINATION_DATE AS rsv_first_date
        ,VACCINATION_STATUS AS rsv_first_status
        ,AGE_AT_EVENT as  rsv_first_event_age
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}
    WHERE VACCINE_ID in ('RSV_1','RSV_1B') and VACCINATION_STATUS in ('Completed', 'OutofSchedule')  