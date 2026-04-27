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
,DATE(i.clinical_effective_date) as illicit_drug_date
,i.illicit_drug_assessed_last_12m
,i.illicit_drug_pattern
,i.illicit_drug_class
,IFF(i.ILLICIT_DRUG_PATTERN = 'Does not misuse drugs', 'No', 'Yes') AS drug_misuse_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_illicit_drug_latest i
FROM {{ ref('int_smi_illicit_drug_latest') }} i
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (person_id)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--Subs misuse interventions lATEST EVER
,SUBS_MISUSE_INT as (
select 
sm.person_id
,DATE(sm.clinical_effective_date) as sm_int_date
,sm.CONCEPT_DISPLAY as sm_int_type
,sm.subs_misuse_services
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
,s.smoking_status
,IFF(s.LATEST_SMOKING_DATE >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS smok_status_last_12m
,CASE
WHEN s.SMOKING_STATUS = 'Current Smoker' THEN 'Yes' 
WHEN s.SMOKING_STATUS in ('Ex-Smoker','Never Smoked') THEN 'No'
END AS smoker_flag
--FROM REPORTING.OLIDS_PERSON_STATUS.FCT_PERSON_SMOKING_STATUS s
FROM {{ ref('fct_person_smoking_status') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)

UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as smoking_date
,'Smoking Status Declined' AS smoking_status
,IFF(s.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS smok_status_last_12m
,NULL as smoker_flag
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
,DATE(s.clinical_effective_date) as smok_int_date
,s.CONCEPT_DISPLAY as smok_int_type
,s.smoking_cessation_services
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
,a.AUDIT_RISK_CATEGORY as alcohol_risk_category
,CASE
WHEN a.AUDIT_RISK_CATEGORY in ('Possible Dependence','Increasing Risk','Higher Risk') THEN 'Yes'
WHEN a.AUDIT_RISK_CATEGORY in ('Occasional Drinker','Lower Risk','Non-Drinker') THEN 'No'
END AS high_alcohol_use_flag
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
,a.high_alcohol_use_flag
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
,'Alcohol Status Declined' AS alcohol_risk_category
,NULL as high_alcohol_use_flag
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
,DATE(ai.clinical_effective_date) as alc_int_date
,ai.CONCEPT_DISPLAY as alc_int_type
,ai.alcohol_advice_services
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
,DATE(n.clinical_effective_date) as nutr_rev_date
,n.CONCEPT_DISPLAY as nutr_rev_outcome
,n.poor_diet_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_nutrition_review_latest n 
FROM {{ ref('int_smi_longlives_nutrition_review_latest') }} n
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--WEIGHT MGMT INTERVENTION LATEST EVER
,WT_MGMT as (
select 
w.person_id
,DATE(w.clinical_effective_date) as wt_mgmt_date
,w.CONCEPT_DISPLAY as wt_mgmt_type
,w.referral_diet_advice
,w.referral_exercise_advice
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_weight_mgmt_latest w
FROM {{ ref('int_smi_longlives_weight_mgmt_latest') }} w
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--DENTAL INSPECTION LATEST EVER
,DENTAL_INSPECTION as (
select 
d.person_id
,DATE(d.clinical_effective_date) as dental_date
,d.CONCEPT_DISPLAY as dental_type
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
,e.low_exercise_flag
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
,i.drug_misuse_flag
,sm.SM_INT_DATE
,sm.SM_INT_TYPE
,sm.subs_misuse_services
--smoking status and interventions
,ss.smok_status_last_12m
,ss.SMOKING_DATE as SMOK_STATUS_DATE
,ss.SMOKING_STATUS
,ss.smoker_flag
,si.SMOK_INT_DATE
,si.SMOK_INT_TYPE
,si.smoking_cessation_services
--alcohol use ever and alcohol interventions
,a.alcohol_assessment_date as ALC_STAT_DATE
,a.ALCOHOL_RISK_CATEGORY
,a.ALCOHOL_UNITS
,a.UNIT_DISPLAY
,a.high_alcohol_use_flag
,ai.ALC_INT_DATE
,ai.ALC_INT_TYPE
,ai.alcohol_advice_services
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
