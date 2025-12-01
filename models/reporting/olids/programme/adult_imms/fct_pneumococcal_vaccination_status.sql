
{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
WITH
-- All eligible people (from adult current vaccination population meeting age 65+ ) 
eligible AS (
    SELECT 
        person_id
        ,age
    FROM {{ ref('int_adult_imms_current_population') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENT_POPULATION
    where age >= 65
)

-- PPV SINGLE DOSE (PPV or PCV - starting 2026)
,ppv as (
select *
from 
    (
--Single Dose given
    SELECT 
        person_id
        ,'Pneumococcal' As campaign
        ,vaccination_date
       ,'VACCINATION_ADMINISTERED' as vaccination_status
    FROM {{ ref('int_adult_imms_ppv_vaccination_given') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_PPV_VACCINATION_GIVEN
    where vaccination_status is not null
UNION
--Single Dose declined
    SELECT 
        person_id
         ,'Pneumococcal' As campaign
        ,vaccination_date
       ,'VACCINATION_DECLINED' as vaccination_status
    FROM {{ ref('int_adult_imms_ppv_vaccination_declined') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_PPV_VACCINATION_DECLINED
   where vaccination_status is not null
) a
order by 1,3
) 

select e.*
,CASE WHEN p.campaign is null THEN 'NO_VAX_RECORD' ELSE p.campaign END AS campaign
,p.vaccination_date
,CASE
WHEN p.vaccination_status is null THEN 'NO_VACCINATION_RECORD' ELSE p.vaccination_status END AS vaccination_status
from eligible e
LEFT JOIN ppv p using (person_id)