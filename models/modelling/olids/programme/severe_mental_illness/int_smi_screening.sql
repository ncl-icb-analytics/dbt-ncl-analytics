{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--SMI SCREENING STATUS cervial, bowel and breast
WITH CERVICAL_STATUS AS (
select 
c.person_id
,'Cervical' as SCREENING_TYPE
,DATE(c.LATEST_SCREENING_DATE) as LATEST_SCREEN_DATE
,CASE
WHEN c.LATEST_SCREENING_TYPE = 'Cervical Screening Completed' THEN 'Screening Completed' ELSE c.LATEST_SCREENING_TYPE END as LATEST_SCREEN_OUTCOME
,CASE
WHEN c.NEVER_SCREENED = TRUE THEN 'YES' ELSE 'NO' END AS NEVER_HAD_SCREEN
,DATE(c.latest_completed_date) AS LAST_COMPLETED_DATE
-- FROM DEV__REPORTING.OLIDS_PROGRAMME.FCT_CERVICAL_SCREENING_STATUS c
FROM {{ ref('fct_cervical_screening_status') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
-- INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
WHERE P.GENDER = 'Female'
)
,BREAST_STATUS AS (
select 
c.person_id
,'Breast' as SCREENING_TYPE
,DATE(c.LATEST_SCREENING_DATE) as LATEST_SCREEN_DATE
,CASE 
WHEN c.LATEST_SCREENING_TYPE = 'Breast Screening Completed' THEN 'Screening Completed' ELSE c.LATEST_SCREENING_TYPE END as LATEST_SCREEN_OUTCOME
,CASE
WHEN c.NEVER_SCREENED = TRUE THEN 'YES' ELSE 'NO' END AS NEVER_HAD_SCREEN
,DATE(c.latest_completed_date) AS LAST_COMPLETED_DATE
-- FROM DEV__REPORTING.OLIDS_PROGRAMME.FCT_BREAST_SCREENING_STATUS c
FROM {{ ref('fct_breast_screening_status') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
-- INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
WHERE P.GENDER = 'Female'
)

,BOWEL_STATUS AS (
select 
c.person_id
,'Bowel' as SCREENING_TYPE
,DATE(c.LATEST_SCREENING_DATE) as LATEST_SCREEN_DATE
,CASE
WHEN c.LATEST_SCREENING_TYPE = 'Bowel Screening Completed' THEN 'Screening Completed' ELSE c.LATEST_SCREENING_TYPE END as LATEST_SCREEN_OUTCOME
,CASE
WHEN c.NEVER_SCREENED = TRUE THEN 'YES' ELSE 'NO' END AS NEVER_HAD_SCREEN
,DATE(c.latest_completed_date) AS LAST_COMPLETED_DATE
--FROM DEV__REPORTING.OLIDS_PROGRAMME.FCT_BOWEL_SCREENING_STATUS c
FROM {{ ref('fct_bowel_screening_status') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
)
,COMBINED AS (
Select * 
FROM (
select c.*
from CERVICAL_STATUS c

UNION

select br.*
from BREAST_STATUS br

UNION

select b.*
from BOWEL_STATUS b
)
order by 1
)

select 
p.person_id
,p.gender
,p.age
,c.screening_type 
,c.latest_screen_date
,c.latest_screen_outcome
,c.never_had_screen
,c.last_completed_date

FROM {{ ref('int_smi_population_base') }} p
LEFT JOIN COMBINED c using (person_id)
   