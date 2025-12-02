{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
WITH
-- All eligible people (from adult current vaccination population meeting age 65+ or catch up campaign 70-79)
eligible AS (
    SELECT 
        person_id
        ,age
       ,TRUE as eligible
        ,TURN_65_AFTER_SEP_2023
    FROM {{ ref('int_adult_imms_current_population') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENT_POPULATION
    where age between 70 and 79 or TURN_65_AFTER_SEP_2023 = 'YES'
)

-- SHINGLES DOSE 1
,shingles_1 as (
select *
from 
    (
--dose 1 given 
    SELECT 
        person_id
        ,'Shingles Dose 1' As campaign
        ,shing_first_date AS vaccination_date
       ,'VACCINATION_ADMINISTERED' as vaccination_status
    FROM {{ ref('int_adult_imms_shingles_vaccination_given') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_SHINGLES_VACCINATION_GIVEN
    where shing_first_status is not null
UNION
--dose 1 declined
    SELECT 
        person_id
        ,'Shingles Dose 1' As campaign
        ,shing_first_date AS vaccination_date
        ,'VACCINATION_DECLINED' as vaccination_status
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_SHINGLES_VACCINATION_DECLINED
    FROM {{ ref('int_adult_imms_shingles_vaccination_declined') }}
    where shing_first_status is not null
) a
)
-- SHINGLES DOSE 2
,shingles_2 as (
select *
from 
    (
 --dose 2 given 
    SELECT 
        person_id
        ,'Shingles Dose 2' As campaign
        ,shing_second_date AS vaccination_date
       ,'VACCINATION_ADMINISTERED' as vaccination_status
    FROM {{ ref('int_adult_imms_shingles_vaccination_given') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_SHINGLES_VACCINATION_GIVEN
    where shing_second_status is not null
UNION
--dose 2 declined
    SELECT 
        person_id
       ,'Shingles Dose 2' As campaign
        ,shing_second_date AS vaccination_date
        ,'VACCINATION_DECLINED' as vaccination_status
   --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_SHINGLES_VACCINATION_DECLINED
    FROM {{ ref('int_adult_imms_shingles_vaccination_declined') }}
    where shing_second_status is not null
) a
)
,COMBINED as (
select * 
from (
select * 
from 
shingles_1 
union
select * 
from 
shingles_2
)
order by 1,3
)

select e.*
,CASE WHEN c.campaign is null THEN 'Shingles Dose 1' ELSE c.campaign END AS campaign 
,c.vaccination_date
,CASE
WHEN c.vaccination_status is null THEN 'NO_VACCINATION_RECORD' ELSE c.vaccination_status END AS vaccination_status
from eligible e
LEFT JOIN combined C using (person_id)