{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}


--ENHANCED METRICS - WORK IN PROGRESS

--Care PLan in the last year - only in QOF
WITH latest_Care as (
select 
m.person_id
,DATE(m.clinical_effective_date) as care_plan_date
,m.MH_CARE_PLAN_CURRENT_12M
FROM {{ ref('int_smi_care_plan_latest') }} m
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where m.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
)
--QRISK
,qrisk as (
select 
q.person_id
,DATE(q.clinical_effective_date) as QRISK_DATE
,q.cvd_risk_category
FROM {{ ref('int_qrisk_latest') }} q
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)

--illicit drug use
,illicit as (
select 
i.person_id
,DATE(i.clinical_effective_date) as ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_ASSESSED_LAST_12M
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
FROM {{ ref('int_smi_illicit_drug_latest') }} i
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)

--flu if eligible (morbidly obese etc) and covid (should be age 75+, Imm supp and care home only)
,covidflu as (
select distinct
  s.person_id
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null then TRUE else FALSE end as ELIGIBLE_FLU
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null then TRUE else FALSE end as ELIGIBLE_COVID
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN f.VACCINATION_STATUS END AS FLU_VACC_STATUS
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN DATE(f.VACCINATION_DATE) END AS FLU_VACC_DATE
 ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN c.VACCINATION_STATUS END AS COVID_VACC_STATUS
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN DATE(c.VACCINATION_DATE) END AS COVID_VACC_DATE
  from {{ ref('int_smi_population_base')  }} s
  left join {{ ref('covid_flu_dashboard_base') }} f on s.person_id = f.person_id
  and F.campaign_id in ('Flu 2025-26') and F.is_active = true
   left join {{ ref('covid_flu_dashboard_base') }} c on s.person_id = c.person_id
        AND C.campaign_id in ('COVID Autumn 2025') and c.is_active = true
)
--Population demographics + enhanced metrics wide table
select 
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_BOROUGH 
,p.PRACTICE_NEIGHBOURHOOD
,p.PRIMARY_CARE_NETWORK
,p.PRACTICE_NAME 
,p.PRACTICE_CODE
,p.RESIDENTIAL_BOROUGH
,p.WARD_CODE
,p.WARD_NAME
,p.AGE
,p.GENDER
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.IMD_QUINTILE
,p.IMDQUINTILE_ORDER
,p.MAIN_LANGUAGE
,q.QRISK_DATE
,q.CVD_RISK_CATEGORY
,i.ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_ASSESSED_LAST_12M
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
,m.care_plan_date
-- ,(
--   CASE WHEN BMI_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
--   CASE WHEN HBA1C_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
--   CASE WHEN CHOLESTEROL_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
--   CASE WHEN SMOKING_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
--   CASE WHEN (AUDIT_CHECK_12M = 'Yes' OR ALCOHOL_CHECK_12M = 'Yes') THEN 1 ELSE 0 END +
--   CASE WHEN BP_CHECK_12M = 'Yes' THEN 1 ELSE 0 END 
--   -- + CASE WHEN CARE_PLAN_CHECK_12M = 'Yes' THEN 1 ELSE 0 END 
-- ) AS SIX_COMP_COUNT

from {{ ref('int_smi_population_base')  }} p
LEFT JOIN latest_Care m using (person_id)
LEFT JOIN qrisk q using (person_id)
LEFT JOIN illicit i using (person_id)
-- LEFT JOIN covidflu cf using (person_id)