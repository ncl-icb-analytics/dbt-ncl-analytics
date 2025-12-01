{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

WITH CERVICAL_STATUS AS (
--WITH CERVICAL_STATUS
select 
c.person_id
,'Cervical' as SCREENING_TYPE
,DATE(c.LATEST_SCREENING_DATE) as LATEST_SCREEN_DATE
,c.LATEST_SCREENING_TYPE as LATEST_SCREEN_OUTCOME
,CASE
WHEN c.NEVER_SCREENED = TRUE THEN 'YES' ELSE 'NO' END AS NEVER_HAD_SCREEN
,DATE(c.latest_completed_date) AS LATEST_SCREEN_COMPLETED_DATE
FROM {{ ref('fct_cervical_screening_status') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
WHERE P.GENDER = 'Female'
)

SELECT
P.PERSON_ID
,C.LATEST_SCREEN_DATE
,C.LATEST_SCREEN_OUTCOME
,C.NEVER_HAD_SCREEN
,C.LATEST_SCREEN_COMPLETED_DATE
FROM {{ ref('int_smi_population_base')  }} P
LEFT JOIN CERVICAL_STATUS C USING (PERSON_ID)
   