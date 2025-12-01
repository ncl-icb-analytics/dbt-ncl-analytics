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

from {{ ref('int_smi_population_base')  }} p
LEFT JOIN latest_Care m using (person_id)
LEFT JOIN qrisk q using (person_id)
LEFT JOIN illicit i using (person_id)
