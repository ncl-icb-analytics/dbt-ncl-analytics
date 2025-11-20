{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 65+ who are eligible who have declined PPV vaccine. One dose only needed. Latest vaccination recorded
 SELECT 
        PERSON_ID
        ,AGE AS CURRENT_AGE
        ,VACCINE_ID
        ,VACCINATION_DATE 
        ,VACCINATION_STATUS 
	    ,AGE_AT_EVENT 
        FROM {{ ref('int_adult_imms_vaccination_status_current') }} 
    WHERE VACCINE_ID = 'PPV_1' AND VACCINATION_STATUS in ('Declined')