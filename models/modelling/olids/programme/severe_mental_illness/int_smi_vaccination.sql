{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--SMI VACCINATION STATUS COVID AND FLU
--flu if eligible (morbidly obese etc) and covid (should be age 75+, Imm supp and care home only)
WITH FLU AS (
select 
  s.person_id
  ,f.campaign_id as vaccination_type
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null then TRUE else FALSE end as ELIGIBLE
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN f.VACCINATION_STATUS END AS VACCINATION_STATUS
   ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN DATE(f.VACCINATION_DATE) END AS VACCINATION_DATE
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  FROM {{ ref('int_smi_population_base')  }} s
  --LEFT JOIN DEV__PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_COVID_FLU.COVID_FLU_DASHBOARD_BASE f using (person_id)
  LEFT JOIN {{ ref('covid_flu_dashboard_base') }} f using (person_id)
  WHERE F.campaign_id in ('Flu 2025-26') and F.is_active = true
  )
  ,COVID AS (
  select 
  s.person_id
  ,c.campaign_id as vaccination_type
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null then TRUE else FALSE end as ELIGIBLE
 ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN c.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN DATE(c.VACCINATION_DATE) END AS VACCINATION_DATE
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('covid_flu_dashboard_base') }} c using (person_id)
  --LEFT JOIN DEV__PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_COVID_FLU.COVID_FLU_DASHBOARD_BASE c using (person_id)
 WHERE C.campaign_id in ('COVID Autumn 2025') and c.is_active = true
)
,COMBINED as (
Select * 
FROM (
select f.*
from FLU f

UNION

select c.*
from COVID c

)
order by 1
)

select 
p.person_id
,p.gender
,c.vaccination_type 
,c.eligible
,c.vaccination_status
,c.vaccination_date
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN COMBINED c using (person_id)