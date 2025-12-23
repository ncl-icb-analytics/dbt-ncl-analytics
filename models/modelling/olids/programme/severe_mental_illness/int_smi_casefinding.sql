{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--Create a person level dataset for where the key filters are number of checks missed in last 12 months and vulnerabilities such as smoking, substance misuse, homelessness, deprivation and LTC count
--number of incomplete or declined health checks in last 12 months
with healthcheck_sum as (
SELECT DISTINCT
  PERSON_ID
  ,LISTAGG(CASE 
  WHEN CHECK_TYPE = 'Smoking' THEN 'Smok'
  WHEN CHECK_TYPE = 'Blood Pressure' THEN 'BP'
  WHEN CHECK_TYPE = 'Alcohol' THEN 'Alc'
  WHEN CHECK_TYPE = 'Cholesterol' THEN 'Chol'
  WHEN CHECK_TYPE = 'HBA1C' THEN 'HbA1c'
  ELSE CHECK_TYPE END ,',') AS INCOMP12M_LIST
   ,COUNT(DISTINCT CHECK_TYPE) AS INCOMP12M_CT
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_SIX_HEALTH_CHECK 
FROM {{ ref('int_smi_six_health_check')  }} 
WHERE check_status in ('Not Met','Declined')
Group by ALL
)
--FIND PEOPLE WHO HAVE DECLINED A HEALTH CHECK
,declined as (
select distinct person_id
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_SIX_HEALTH_CHECK
FROM {{ ref('int_smi_six_health_check')  }}
WHERE check_status = 'Declined' OR exception_category is not null
)
select 
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_NAME
,p.PRACTICE_CODE
,hc.INCOMP12M_CT
,hc.INCOMP12M_LIST
,CASE WHEN dc.PERSON_ID IS NOT NULL THEN 'Yes' ELSE 'No' END AS HAS_DECLINED
,AGE
,GENDER
,ETHNICITY_CATEGORY
,ETHCAT_ORDER
,ETHNICITY_SUBCATEGORY
,ETHSUBCAT_ORDER
,ETHNICITY_GRANULAR
,MAIN_LANGUAGE
,INTERPRETER_TYPE
,CASE WHEN IS_HOMELESS = TRUE THEN 'Yes' ELSE 'No' END AS IS_HOMELESS
,IMD_QUINTILE
,IMDQUINTILE_ORDER
,CASE WHEN INTERPRETER_NEEDED = TRUE THEN 'Yes' ELSE 'No' END AS INTERPRETER_NEEDED
,CASE WHEN l.smoking_status = 'Current Smoker' THEN 'Yes' ELSE 'No' END AS IS_SMOKER
,CASE
WHEN l.illicit_drug_pattern in ('Abstinence/Remission','Overdose or Poisoning','Dependence','Injecting drug user','Misuse/Harmful Use','Withdrawal/Treatment','Drug-Induced Mental Disorders') THEN 'Hx harmful use'
WHEN l.illicit_drug_pattern is NULL THEN 'Unknown' ELSE l.illicit_drug_pattern END as DRUG_USE
,l.illicit_drug_date AS LATEST_DRUG_DATE
,l.ALCOHOL_RISK_CATEGORY AS LATEST_ALCOHOL_EVER
,l.alc_stat_date AS LATEST_ALCOHOL_DATE
,NVL(ltc.LTC_COUNT,0) AS LTC_COUNT
,CASE WHEN ltc.LTC_COUNT >= 2 THEN 'Yes' ELSE 'No' END AS LTC_2PLUS
,ltc.LTC_SUMMARY
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN healthcheck_sum hc using (person_id)
LEFT JOIN declined dc using (person_id)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_LIFESTYLE l using (person_id)
LEFT JOIN {{ ref('int_smi_lifestyle')  }} l using (person_id)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_LTC_PROFILE ltc using (person_id)
LEFT JOIN {{ ref('int_smi_ltc_profile')  }} ltc using (person_id)
WHERE hc.PERSON_ID IS NOT NULL 
