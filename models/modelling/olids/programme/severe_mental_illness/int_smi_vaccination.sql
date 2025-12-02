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
  ,s.age
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
  ,s.age
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
--PPV age 65+
,PPV as (
select 
  s.person_id
  ,s.age
  ,p.campaign AS vaccination_type
  ,CASE 
  WHEN p.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN p.campaign IS NOT NULL and p.person_id is not null THEN p.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN p.campaign IS NOT NULL and p.person_id is not null THEN DATE(p.VACCINATION_DATE) END AS VACCINATION_DATE
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_pneumococcal_vaccination_status') }} p using (person_id)
  --LEFT JOIN DEV__REPORTING.OLIDS_PROGRAMME.FCT_PNEUMOCOCCAL_VACCINATION_STATUS p using (person_id)
  WHERE p.person_id is not null
)
--age 75+ and catch up campaign
,RSV as (
select 
  s.person_id
  ,s.age
  ,r.campaign AS vaccination_type
  ,CASE 
  WHEN r.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN r.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN DATE(r.VACCINATION_DATE) END AS VACCINATION_DATE
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_rsv_vaccination_status') }} r using (person_id)
  --LEFT JOIN DEV__REPORTING.OLIDS_PROGRAMME.FCT_RSV_VACCINATION_STATUS r using (person_id)
  WHERE r.person_id is not null
)
--Age 65+ and catch up campaign
,SHING as (
select 
  s.person_id
  ,s.age
  ,r.campaign AS vaccination_type
  ,CASE 
  WHEN r.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN r.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN DATE(r.VACCINATION_DATE) END AS VACCINATION_DATE
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_shingles_vaccination_status') }} r using (person_id)
  --LEFT JOIN DEV__REPORTING.OLIDS_PROGRAMME.FCT_SHINGLES_VACCINATION_STATUS r using (person_id)
  WHERE r.person_id is not null
)
,COMBINED as (
Select * 
FROM (
select f.*
from FLU f

UNION

select c.*
from COVID c

UNION
select p.*
from PPV p

UNION
select r.*
from RSV r

UNION
select s.*
from SHING s
)
order by 1
)

select 
p.person_id
,p.gender
,p.age
,c.vaccination_type 
,c.eligible
,c.vaccination_status
,c.vaccination_date
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN COMBINED c using (person_id)