{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--COMPILE LIFESTYLE PERSON LEVEL DATA
--illicit drug use LATEST EVER
WITH illicit as (
select 
i.person_id
,DATE(i.clinical_effective_date) as ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_ASSESSED_LAST_12M
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_illicit_drug_latest i
FROM {{ ref('int_smi_illicit_drug_latest') }} i
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (person_id)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--Subs misuse interventions lATEST EVER
,SUBS_MISUSE_INT as (
select 
sm.person_id
,DATE(sm.clinical_effective_date) as SM_INT_DATE
,sm.CONCEPT_DISPLAY as SM_INT_TYPE
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_subs_misuse_intervention_latest sm 
FROM {{ ref('int_smi_longlives_subs_misuse_intervention_latest') }} sm
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
-- SMOKING STATUS LATEST EVER
,SMOK_EVER as (
SELECT * 
FROM (
select 
s.person_id
,DATE(s.LATEST_SMOKING_DATE) as smoking_date
,s.SMOKING_STATUS
--FROM REPORTING.OLIDS_PERSON_STATUS.FCT_PERSON_SMOKING_STATUS s
FROM {{ ref('fct_person_smoking_status') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)

UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as smoking_date
,'Smoking Status Declined' AS SMOKING_STATUS
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_SMOKING_DECLINED s
FROM {{ ref('int_smi_smoking_declined') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.SMOKING_date DESC, CASE WHEN a.SMOKING_STATUS <> 'Smoking Status Declined' THEN 1 ELSE 0 END DESC) = 1
)
--Smoking interventions LATEST EVER
,SMOK_INT as (
select 
s.person_id
,DATE(s.clinical_effective_date) as SMOK_INT_DATE
,s.CONCEPT_DISPLAY as SMOK_INT_TYPE
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_LONGLIVES_SMOKING_INTERVENTION_latest s 
FROM {{ ref('int_smi_longlives_smoking_intervention_latest') }} s 
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--ALCOHOL ASSESSMENT EITHER AUDIT C or UNITS PER WEEK EVER 
,ALC_EVER as (
SELECT * 
FROM (
select 
a.person_id
,DATE(a.LATEST_AUDIT_DATE) as alcohol_assessment_date
,a.AUDIT_RISK_CATEGORY as ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
--FROM REPORTING.OLIDS_PERSON_STATUS.fct_person_alcohol_status a
FROM {{ ref('fct_person_alcohol_status') }} a
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)

UNION

select 
a.person_id
,a.clinical_effective_date as alcohol_assessment_date
,a.alcohol_risk_category
,a.result_value as alcohol_units
,a.result_unit_display as unit_display
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_alcohol_latest a
FROM {{ ref('int_smi_alcohol_latest') }} a
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)


UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as alcohol_assessment_date
,'Alcohol Status Declined' AS ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_ALCOHOL_DECLINED s
FROM {{ ref('int_smi_alcohol_declined') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.alcohol_assessment_date DESC, 
CASE WHEN ALCOHOL_RISK_CATEGORY NOT IN ('Unclear','Alcohol Status Declined') THEN 2 
WHEN ALCOHOL_RISK_CATEGORY = 'Alcohol Status Declined' THEN 1 ELSE 0 END DESC) = 1
)
--ALCOHOL INTERVENTION LATEST EVER
,ALCOHOL_INT as (
select 
ai.person_id
,DATE(ai.clinical_effective_date) as ALC_INT_DATE
,ai.CONCEPT_DISPLAY as ALC_INT_TYPE
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_alcohol_intervention_latest ai 
FROM {{ ref('int_smi_longlives_alcohol_intervention_latest') }} ai
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--BMI latest EVER
,BMI_EVER as (
select * 
FROM (
--latest BMI code EVER
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_date
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_BMI_LATEST b
FROM {{ ref('int_bmi_latest') }} b
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)

UNION

--latest BMI declined EVER
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_DATE
,'BMI Declined' as bmi_category
,NULL as BMI_VALUE
,7 as BMI_RISK_SORT_KEY
--FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_BMI_DECLINED b
FROM {{ ref('int_smi_bmi_declined') }} b
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
--select latest from BMI code or Declined LATEST EVER
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.bmi_date DESC, CASE WHEN a.bmi_category <> 'BMI Declined' THEN 1 ELSE 0 END DESC) = 1
)
--NUTRITION REVIEW LATEST EVER
,NUT_REV as (
select 
n.person_id
,DATE(n.clinical_effective_date) as NUTR_REV_DATE
,n.CONCEPT_DISPLAY as NUTR_REV_OUTCOME
,CASE 
WHEN n.concept_code in ( '310502008','301991000000101') THEN 'Yes' 
WHEN n.CONCEPT_CODE IN ('226234005', '310503003','16208003','301961000000107','310500000') THEN 'No'
WHEN n.CONCEPT_CODE IN ('391129005', '401070008','391132008') THEN 'Outcome Unknown'
END AS POOR_DIET_FLAG
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_nutrition_review_latest n 
FROM {{ ref('int_smi_longlives_nutrition_review_latest') }} n
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--WEIGHT MGMT INTERVENTION LATEST EVER
,WT_MGMT as (
select 
w.person_id
,DATE(w.clinical_effective_date) as WT_MGMT_DATE
,w.CONCEPT_DISPLAY as WT_MGMT_TYPE
,CASE
WHEN w.CONCEPT_CODE IN ('103699006', '306163007','443288003','11816003') THEN 'Yes'
END AS REFERRAL_DIET_ADVICE
,CASE
WHEN w.CONCEPT_CODE IN ('390893007', '892281000000101','390893007','304507003','526151000000109','183073003','416974006') THEN 'Yes'
END AS REFERRAL_EXERCISE_ADVICE
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_weight_mgmt_latest w
FROM {{ ref('int_smi_longlives_weight_mgmt_latest') }} w
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--DENTAL INSPECTION LATEST EVER
,DENTAL_INSPECTION as (
select 
d.person_id
,DATE(d.clinical_effective_date) as DENTAL_DATE
,d.CONCEPT_DISPLAY as DENTAL_TYPE
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_dental_inspection_latest d
FROM {{ ref('int_smi_longlives_dental_inspection_latest') }} d
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--EXERCISE PATTERN LATEST EVER
,EXERCISE as (
select 
e.person_id
,DATE(e.clinical_effective_date) as EX_STAT_DATE
,e.CONCEPT_DISPLAY as EXERCISE_STATUS
,CASE 
WHEN e.CONCEPT_CODE IN ('228445002','160631001') THEN 'Yes' 
WHEN e.CONCEPT_CODE IN ('160632008','160633003') THEN 'No'
WHEN e.CONCEPT_CODE IN ('160628002','266930008') THEN 'Outcome Unknown'
ELSE e.CONCEPT_CODE END AS LOW_EXERCISE_FLAG
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_exercise_assessment_latest e
FROM {{ ref('int_smi_longlives_exercise_assessment_latest') }} e
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--Population demographics and lifestyle factors
SELECT
p.PERSON_ID
,p.HX_FLAKE
,p.AGE
,p.GENDER
--latest illicit drug use and subs misuse service referrals
,i.ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
--smoking status ever and smoking cessation referrals
,sm.SM_INT_DATE
,sm.SM_INT_TYPE
,ss.SMOKING_DATE as SMOK_STATUS_DATE
,ss.SMOKING_STATUS
,si.SMOK_INT_DATE
,si.SMOK_INT_TYPE
--alcohol use ever and alcohol interventions
,a.alcohol_assessment_date as ALC_STAT_DATE
,a.ALCOHOL_RISK_CATEGORY
,a.ALCOHOL_UNITS
,a.UNIT_DISPLAY
,ai.ALC_INT_DATE
,ai.ALC_INT_TYPE
--weight management
,b.BMI_DATE
,b.BMI_CATEGORY
,n.NUTR_REV_DATE
,n.NUTR_REV_OUTCOME
,n.POOR_DIET_FLAG
,w.REFERRAL_DIET_ADVICE
,w.WT_MGMT_DATE
,w.WT_MGMT_TYPE
,d.DENTAL_DATE
,d.DENTAL_TYPE
,e.EX_STAT_DATE
,e.EXERCISE_STATUS
,e.LOW_EXERCISE_FLAG
,w.REFERRAL_EXERCISE_ADVICE
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN ILLICIT i using (person_id)
LEFT JOIN SUBS_MISUSE_INT sm using (person_id)
LEFT JOIN SMOK_EVER ss using (person_id)
LEFT JOIN SMOK_INT si using (person_id)
LEFT JOIN ALC_EVER a using (person_id)
LEFT JOIN ALCOHOL_INT ai using (person_id)
LEFT JOIN BMI_EVER b using (person_id)
LEFT JOIN NUT_REV n using (person_id)
LEFT JOIN WT_MGMT w using (person_id)
LEFT JOIN DENTAL_INSPECTION d using (person_id)
LEFT JOIN EXERCISE e using (person_id)
