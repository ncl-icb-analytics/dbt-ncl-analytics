{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--RSV NOTE MISSING ADMIN CODE from concept map means vaccination given numbers are very low
WITH
-- All eligible people (from adult current vaccination population aged 75 to 79, or turned 80 after 1 September 2024)
eligible AS (
    SELECT 
        person_id
        ,age
       ,TRUE as eligible
        ,TURN_80_AFTER_SEP_2024
    FROM {{ ref('int_adult_imms_current_population') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENT_POPULATION
    where age between 75 and 79 or TURN_80_AFTER_SEP_2024 = 'YES'
)
-- RSV SINGLE DOSE
,rsv as (
select *
from 
    (
--Single Dose given
    SELECT 
        person_id
        ,'RSV' As campaign
        ,rsv_first_date as vaccination_date
       ,'VACCINATION_ADMINISTERED' as vaccination_status
    FROM {{ ref('int_adult_imms_rsv_vaccination_given') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_RSV_VACCINATION_GIVEN
    where rsv_first_status is not null
UNION
--Single Dose declined
    SELECT 
        person_id
         ,'RSV' As campaign
        ,rsv_first_date as vaccination_date
       ,'VACCINATION_DECLINED' as vaccination_status
    FROM {{ ref('int_adult_imms_rsv_vaccination_declined') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_RSV_VACCINATION_DECLINED
   where rsv_first_status is not null
) a
order by 1,3
) 

select e.*
,CASE WHEN p.campaign is null THEN 'RSV' ELSE p.campaign END AS campaign 
,p.vaccination_date
,CASE
WHEN p.vaccination_status is null THEN 'NO_VACCINATION_RECORD' ELSE p.vaccination_status END AS vaccination_status
from eligible e
LEFT JOIN rsv p using (person_id)