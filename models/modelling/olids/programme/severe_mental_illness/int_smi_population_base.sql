{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

--SMI REGISTER HEALTH CHECKS
WITH SMI_POP AS (
select
dem.PERSON_ID
,ID.HX_FLAKE
,dem.SK_PATIENT_ID
,dem.AGE
,dem.GENDER
,CASE
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 'Unknown'
ELSE dem.ETHNICITY_CATEGORY END AS ETHNICITY_CATEGORY
,CASE 
WHEN dem.ETHNICITY_CATEGORY = 'Asian' THEN 1
WHEN dem.ETHNICITY_CATEGORY = 'Black' THEN 2
WHEN dem.ETHNICITY_CATEGORY = 'Mixed' THEN 3
WHEN dem.ETHNICITY_CATEGORY = 'Other' THEN 4
WHEN dem.ETHNICITY_CATEGORY = 'White' THEN 5
WHEN dem.ETHNICITY_CATEGORY = 'Unknown' THEN 6
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 6
END AS ETHCAT_ORDER 
,CASE
WHEN dem.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
ELSE dem.ETHNICITY_SUBCATEGORY END AS ETHNICITY_SUBCATEGORY
,CASE 
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Bangladeshi' THEN 1
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Chinese' THEN 2
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Indian' THEN 3
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Pakistani' THEN 4
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Other Asian' THEN 5
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: African' THEN 6
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Caribbean' THEN 7
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Other Black' THEN 8
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Asian' THEN 9
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black African' THEN 10
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black Caribbean' THEN 11
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: Other Mixed' THEN 12
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Arab' THEN 13
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Other' THEN 14
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: British' THEN 15
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Irish' THEN 16
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Traveller' THEN 17
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Other White' THEN 18
WHEN dem.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Refused' THEN 19
END AS ETHSUBCAT_ORDER
,dem.IMD_QUINTILE_19 AS IMD_QUINTILE
,CASE 
WHEN dem.IMD_QUINTILE_19 = 'Most Deprived' THEN 1
WHEN dem.IMD_QUINTILE_19 = 'Second Most Deprived' THEN 2
WHEN dem.IMD_QUINTILE_19 = 'Third Most Deprived' THEN 3
WHEN dem.IMD_QUINTILE_19 = 'Second Least Deprived' THEN 4
WHEN dem.IMD_QUINTILE_19 = 'Least Deprived' THEN 5
ELSE 6 END AS IMDQUINTILE_ORDER
,dem.IMD_DECILE_19 AS IMD_DECILE
,CASE 
WHEN dem.MAIN_LANGUAGE = 'Pushto' THEN 'Pashto' 
WHEN dem.MAIN_LANGUAGE in ('Makaton sign language','Sign language','British Sign Language') THEN 'Sign language'
WHEN dem.MAIN_LANGUAGE = 'Not Recorded' THEN 'Unknown'
ELSE dem.MAIN_LANGUAGE END AS MAIN_LANGUAGE
,dem.BOROUGH_REGISTERED AS PRACTICE_BOROUGH 
,dem.NEIGHBOURHOOD_REGISTERED AS PRACTICE_NEIGHBOURHOOD
,dem.PCN_NAME AS PRIMARY_CARE_NETWORK
,dem.PRACTICE_NAME 
,dem.PRACTICE_CODE
,COALESCE(la.LAD25_NM,'Unknown') as RESIDENTIAL_BOROUGH
,COALESCE(dem.NEIGHBOURHOOD_RESIDENT,'Unknown') as RESIDENTIAL_NEIGHBOURHOOD
,dem.WARD_CODE
,dem.WARD_NAME
,dem.LSOA_CODE_21
,CASE WHEN la.RESIDENT_FLAG IS NULL THEN 'Unknown'
ELSE la.RESIDENT_FLAG END as RESIDENTIAL_LOC
,ltc.HAS_CORONARY_HEART_DISEASE as HAS_CHD
,ltc.HAS_CHRONIC_KIDNEY_DISEASE as HAS_CKD
,ltc.HAS_DIABETES 
,ltc.HAS_COPD
,ltc.HAS_HYPERTENSION as HAS_HYP
,ltc.HAS_STROKE_TIA as HAS_STIA
,ltc.HAS_PERIPHERAL_ARTERIAL_DISEASE as HAS_PAD
,smi.IS_ON_LITHIUM
,smi.HAS_ACTIVE_SMI_DIAGNOSIS
FROM {{ ref('dim_person_demographics') }} dem 
INNER JOIN {{ ref('fct_person_smi_register') }} smi using (PERSON_ID)
LEFT JOIN {{ ref('dim_person_conditions') }} ltc using (PERSON_ID)
LEFT JOIN {{ ref('person_pseudo') }} AS ID  using (PERSON_ID)
LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = dem.LSOA_CODE_21
where dem.is_active = TRUE
)
--CORE METRICS
--BMI in the last year #1
,latest_BMI as (
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_date
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
FROM {{ ref('int_bmi_latest') }} b
INNER JOIN SMI_POP p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
)
--HbA1c in the last year #2
,latest_HBA as (
select 
h.person_id
,DATE(h.clinical_effective_date) as HBA1C_date
,h.HBA1C_CATEGORY
FROM {{ ref('int_hba1c_latest') }} h
INNER JOIN SMI_POP p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
)

--Total (serum) Cholesterol in the last year #3
,latest_cholesterol as (
select 
c.person_id
,DATE(c.clinical_effective_date) as Cholesterol_date
,c.CHOLESTEROL_CATEGORY
,c.CHOLESTEROL_VALUE
FROM {{ ref('int_cholesterol_latest') }} c
INNER JOIN SMI_POP p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
)

--Smoking Status in the last year#4
,latest_smoking as (
select 
s.person_id
,DATE(s.LATEST_SMOKING_DATE) as smoking_date
,s.SMOKING_STATUS
FROM {{ ref('fct_person_smoking_status') }} s
INNER JOIN SMI_POP p USING (PERSON_ID)
where LATEST_SMOKING_DATE  >= DATEADD('month', -12, CURRENT_DATE)
)

--Alcohol Status in the last year#5
,latest_alcohol_FCT as (
select 
a.person_id
,DATE(a.LATEST_AUDIT_DATE) as audit_date
,a.AUDIT_RISK_CATEGORY
FROM {{ ref('fct_person_alcohol_status') }} a
INNER JOIN SMI_POP p USING (PERSON_ID)
where DATE(a.LATEST_AUDIT_DATE)  >= DATEADD('month', -12, CURRENT_DATE)
)

--Alcohol Status in the last year#5
,latest_alcohol_QOF as (
select 
a.person_id
,a.clinical_effective_date as alcohol_assessment_date
,a.concept_display
,a.alcohol_risk_category
,a.result_value
,a.result_unit_display
FROM {{ ref('int_smi_alcohol_latest') }} a
INNER JOIN SMI_POP p USING (PERSON_ID)
where DATE(clinical_effective_date)  >= DATEADD('month', -12, CURRENT_DATE)
)

--BP in the last year#6
,latest_BP as (
select 
bp.person_id
,DATE(bp.LATEST_BP_DATE) as BP_date
,bp.LATEST_SYSTOLIC_VALUE
,bp.LATEST_DIASTOLIC_VALUE
,bp.IS_OVERALL_BP_CONTROLLED
,bp.IS_DIAGNOSED_HTN
FROM {{ ref('fct_person_bp_control') }} bp
INNER JOIN SMI_POP p USING (PERSON_ID)
where LATEST_BP_DATE  >= DATEADD('month', -12, CURRENT_DATE)
)
--OTHER METRICS
--Care PLan in the last year - only in QOF
,latest_Care as (
select 
m.person_id
,DATE(m.clinical_effective_date) as care_plan_date
,m.MH_CARE_PLAN_CURRENT_12M
FROM {{ ref('int_smi_care_plan_latest') }} m
INNER JOIN SMI_POP p USING (PERSON_ID)
where m.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
)
--QRISK
,qrisk as (
select 
q.person_id
,DATE(q.clinical_effective_date) as QRISK_DATE
,q.cvd_risk_category
FROM {{ ref('int_qrisk_latest') }} q
INNER JOIN SMI_POP p USING (PERSON_ID)
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
INNER JOIN SMI_POP p USING (PERSON_ID)
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
  from SMI_POP s
  left join {{ ref('covid_flu_dashboard_base') }} f on s.person_id = f.person_id
  and F.campaign_id in ('Flu 2025-26') and F.is_active = true
   left join {{ ref('covid_flu_dashboard_base') }} c on s.person_id = c.person_id
        AND C.campaign_id in ('COVID Autumn 2025') and c.is_active = true
)
--combined final
--six health checks plus QRISK
select 
p.*
,q.QRISK_DATE
,q.CVD_RISK_CATEGORY
,CASE WHEN b.person_id is NULL THEN 'No' ELSE 'Yes' END AS BMI_CHECK_12M
,b.BMI_DATE
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
,CASE WHEN h.person_id is NULL THEN 'No' ELSE 'Yes' END AS HBA1C_CHECK_12M
,h.HBA1C_DATE
,h.HBA1C_CATEGORY
,CASE WHEN c.person_id is NULL THEN 'No' ELSE 'Yes' END AS CHOLESTEROL_CHECK_12M
,c.CHOLESTEROL_DATE
,c.CHOLESTEROL_VALUE
,c.CHOLESTEROL_CATEGORY
,CASE WHEN s.person_id is NULL THEN 'No' ELSE 'Yes' END AS SMOKING_CHECK_12M
,s.SMOKING_DATE
,s.SMOKING_STATUS
,CASE WHEN a1.person_id is NULL THEN 'No' ELSE 'Yes' END AS ALCOHOL_CHECK_12M
,a1.alcohol_assessment_date
,a1.alcohol_risk_category
,a1.result_value
,a1.result_unit_display
,CASE WHEN a.person_id is NULL THEN 'No' ELSE 'Yes' END AS AUDIT_CHECK_12M
,a.audit_date
,a.audit_risk_category
,i.ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_ASSESSED_LAST_12M
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
,CASE WHEN bp.person_id is NULL THEN 'No' ELSE 'Yes' END AS BP_CHECK_12M
,bp.bp_date
,bp.LATEST_SYSTOLIC_VALUE
,bp.LATEST_DIASTOLIC_VALUE
,bp.IS_OVERALL_BP_CONTROLLED
,CASE WHEN m.person_id is NULL THEN 'No' ELSE 'Yes' END AS CARE_PLAN_CHECK_12M
,m.care_plan_date
,(
  CASE WHEN BMI_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
  CASE WHEN HBA1C_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
  CASE WHEN CHOLESTEROL_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
  CASE WHEN SMOKING_CHECK_12M = 'Yes' THEN 1 ELSE 0 END +
  CASE WHEN (AUDIT_CHECK_12M = 'Yes' OR ALCOHOL_CHECK_12M = 'Yes') THEN 1 ELSE 0 END +
  CASE WHEN BP_CHECK_12M = 'Yes' THEN 1 ELSE 0 END 
  -- + CASE WHEN CARE_PLAN_CHECK_12M = 'Yes' THEN 1 ELSE 0 END 
) AS SIX_COMP_COUNT

from SMI_POP p
LEFT JOIN latest_BMI b using (person_id)
LEFT JOIN latest_HBA h using (person_id)
LEFT JOIN latest_cholesterol c using (person_id)
LEFT JOIN latest_smoking s using (person_id)
LEFT JOIN latest_alcohol_fct a using (person_id)
LEFT JOIN latest_alcohol_qof a1 using (person_id)
LEFT JOIN latest_BP bp using (person_id)
LEFT JOIN latest_Care m using (person_id)
LEFT JOIN qrisk q using (person_id)
LEFT JOIN illicit i using (person_id)
-- LEFT JOIN covidflu cf using (person_id)