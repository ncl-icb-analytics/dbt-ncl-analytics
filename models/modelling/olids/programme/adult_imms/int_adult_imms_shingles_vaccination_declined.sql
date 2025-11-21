{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 65+ who are eligible who have received Shingles vaccine. Two doses needed. Latest vaccination recorded
--people eligible either turned 65 after Sept 2023 (SHING_1 & SHING_2) or are currently 70-79 for the Catch Up programme (SHING_1B & SHING_2B)
SELECT 
       v1.PERSON_ID 
        ,v1.AGE AS CURRENT_AGE
        ,v1.TURN_65_AFTER_SEP_2023
        ,v1.VACCINE_ID as VACCINE_ID_FIRST
        ,v1.VACCINATION_DATE AS shing_first_date
        ,v1.VACCINATION_STATUS AS shing_first_status
        ,v1.AGE_AT_EVENT as  shing_first_event_age
        ,v2.VACCINE_ID as VACCINE_ID_SECOND
        ,v2.VACCINATION_DATE AS shing_second_date
        ,v2.VACCINATION_STATUS AS shing_second_status
        ,v2.AGE_AT_EVENT as  shing_second_event_age
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}  v1
    LEFT JOIN {{ ref('int_adult_imms_vaccination_status_current') }}  v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID in ('SHING_2','SHING_2B') AND v2.VACCINATION_STATUS in ('Declined')  
    AND v1.VACCINATION_DATE <> v2.VACCINATION_DATE
    WHERE v1.VACCINE_ID in ('SHING_1','SHING_1B') and v1.VACCINATION_STATUS in ('Declined')  