{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
-- Intermediate table holding core health checks for people on the SMI register
/* Intermediate Model to capture 6 measurements including exceptions and declined */
WITH EXCEPTIONS AS (
select 
e.person_id
,DATE(e.clinical_effective_date) as EXCEPTION_DATE
,CASE 
WHEN e.concept_display = 'Excepted from mental health quality indicators - patient unsuitable' THEN 'Patient Unsuitable' ELSE 'Informed dissent' 
END as EXCEPTION_CATEGORY
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_QUALITY_CHECK_EXCEPTION e
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where e.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.person_id ORDER BY e.clinical_effective_date DESC) = 1
)

,BMI as (
select * 
FROM (
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_date
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
FROM MODELLING.OLIDS_OBSERVATIONS.INT_BMI_LATEST b
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_DATE
,'BMI Declined' as bmi_category
,NULL as BMI_VALUE
,7 as BMI_RISK_SORT_KEY
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_BMI_DECLINED b
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where b.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.bmi_date DESC) = 1
)
-- # TEST 2 GLUCOSE/HBA1C
,Glucose as (
select * 
FROM (
select 
b.person_id
,DATE(b.clinical_effective_date) as HBA1C_date
,b.HBA1C_CATEGORY
FROM MODELLING.OLIDS_OBSERVATIONS.INT_HBA1C_LATEST b
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
b.person_id
,DATE(b.clinical_effective_date) as HBA1C_DATE
,'Glucose Test Declined' as HBA1C_CATEGORY
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_GLUCOSE_DECLINED b
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where b.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.HBA1C_date DESC) = 1
)
-- # TEST 3 CHOLESTEROL
,CHOL as (
select * 
FROM (
select 
c.person_id
,DATE(c.clinical_effective_date) as Cholesterol_date
,c.CHOLESTEROL_CATEGORY
,c.CHOLESTEROL_VALUE
FROM MODELLING.OLIDS_OBSERVATIONS.int_cholesterol_latest c
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION
select 
c.person_id
,DATE(c.clinical_effective_date) as Cholesterol_date
,'Cholesterol Test Declined' as CHOLESTEROL_CATEGORY
,NULL as CHOLESTEROL_VALUE
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_CHOLESTEROL_DECLINED c
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where c.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.person_id ORDER BY c.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.Cholesterol_date DESC) = 1
)
-- # TEST 4 BLOOD PRESSURE
,BP as (
SELECT *
FROM (
select 
bp.person_id
,DATE(bp.LATEST_BP_DATE) as BP_date
,bp.LATEST_SYSTOLIC_VALUE
,bp.LATEST_DIASTOLIC_VALUE
,CASE 
WHEN bp.IS_OVERALL_BP_CONTROLLED = TRUE THEN 'Yes'
ELSE 'No' END AS IS_OVERALL_BP_CONTROLLED
FROM REPORTING.OLIDS_MEASURES.FCT_PERSON_BP_CONTROL bp
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where LATEST_BP_DATE  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
bp.person_id
,DATE(bp.clinical_effective_date) as BP_date
,NULL AS LATEST_SYSTOLIC_VALUE
,NULL AS LATEST_DIASTOLIC_VALUE
,'BP Test Declined' AS IS_OVERALL_BP_CONTROLLED
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_BP_DECLINED bp
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where bp.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY bp.person_id ORDER BY bp.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.BP_date DESC) = 1
)
-- # TEST 5 SMOKING STATUS
,SMOK as (
SELECT * 
FROM (
select 
s.person_id
,DATE(s.LATEST_SMOKING_DATE) as smoking_date
,s.SMOKING_STATUS
FROM REPORTING.OLIDS_PERSON_STATUS.FCT_PERSON_SMOKING_STATUS s
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where LATEST_SMOKING_DATE  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as smoking_date
,'Smoking Assessment Declined' AS SMOKING_STATUS
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_SMOKING_DECLINED s
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where s.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.SMOKING_date DESC) = 1
)
--TEST # 6 ALCOHOL ASSESSMENT EITHER AUDIT C or UNITS PER WEEK
,ALC as (
SELECT * 
FROM (
select 
a.person_id
,DATE(a.LATEST_AUDIT_DATE) as alcohol_assessment_date
,a.AUDIT_RISK_CATEGORY as ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
FROM REPORTING.OLIDS_PERSON_STATUS.fct_person_alcohol_status a
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where DATE(a.LATEST_AUDIT_DATE)  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
a.person_id
,a.clinical_effective_date as alcohol_assessment_date
,a.alcohol_risk_category
,a.result_value as alcohol_units
,a.result_unit_display as unit_display
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.int_smi_alcohol_latest a
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where DATE(clinical_effective_date)  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as alcohol_assessment_date
,'Alcohol Assessment Declined' AS ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_ALCOHOL_DECLINED s
INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where s.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.alcohol_assessment_date DESC, CASE WHEN ALCOHOL_RISK_CATEGORY <> 'Unclear' THEN 1 ELSE 0 END DESC) = 1
)
--SUMMARY OF HEALTH CHECKS
,HC_CHECKS AS (
select 
p.person_id
,CASE 
WHEN b.person_id is NULL THEN 'Not Met' 
WHEN b.person_id is NOT NULL AND b.BMI_VALUE IS NULL  THEN 'Declined' 
ELSE 'Met' END AS BMI_CHECK_12M
,CASE 
WHEN h.person_id is NULL THEN 'Not Met' 
WHEN h.person_id is NOT NULL AND h.HBA1C_CATEGORY = 'Glucose Test Declined' THEN 'Declined' 
ELSE 'Met' END AS HBA1C_CHECK_12M
,CASE 
WHEN c.person_id is NULL THEN 'Not Met' 
WHEN c.person_id is NOT NULL AND c.CHOLESTEROL_CATEGORY = 'Cholesterol Test Declined' THEN 'Declined' 
ELSE 'Met' END AS CHOLESTEROL_CHECK_12M
,CASE 
WHEN bp.person_id is NULL THEN 'Not Met' 
WHEN bp.person_id is NOT NULL AND bp.IS_OVERALL_BP_CONTROLLED = 'BP Test Declined' THEN 'Declined' 
ELSE 'Met' END AS BP_CHECK_12M
,CASE 
WHEN s.person_id is NULL THEN 'Not Met' 
WHEN bp.person_id is NOT NULL AND s.SMOKING_STATUS = 'Smoking Assessment Declined' THEN 'Declined' 
ELSE 'Met' END AS SMOKING_CHECK_12M
,CASE 
WHEN a.person_id is NULL THEN 'Not Met' 
WHEN a.person_id is NOT NULL AND a.alcohol_risk_category = 'Alcohol Assessment Declined' THEN 'Declined' 
ELSE 'Met' END AS ALCOHOL_CHECK_12M
FROM {{ ref('int_smi_population_base') }} p
LEFT JOIN BMI b using (person_id)
LEFT JOIN GLUCOSE h using (person_id)
LEFT JOIN CHOL c using (person_id)
LEFT JOIN BP bp using (person_id)
LEFT JOIN SMOK s using (person_id)
LEFT JOIN ALC a using (person_id)
)
--FINAL AS 
select 
p.person_id
,e.EXCEPTION_DATE
,e.EXCEPTION_CATEGORY
,hc.BMI_CHECK_12M
,b.BMI_DATE
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
,h.HBA1C_DATE
,h.HBA1C_CATEGORY
,hc.HBA1C_CHECK_12M
,c.CHOLESTEROL_DATE
,c.CHOLESTEROL_VALUE
,c.CHOLESTEROL_CATEGORY
,hc.CHOLESTEROL_CHECK_12M
,bp.BP_DATE
,bp.LATEST_SYSTOLIC_VALUE
,bp.LATEST_DIASTOLIC_VALUE
,bp.IS_OVERALL_BP_CONTROLLED
,hc.BP_CHECK_12M
,s.SMOKING_DATE
,s.SMOKING_STATUS
,hc.SMOKING_CHECK_12M
,a.alcohol_assessment_date
,a.alcohol_risk_category
,a.alcohol_units
,a.unit_display
,hc.ALCOHOL_CHECK_12M
,(
  CASE WHEN hc.BMI_CHECK_12M = 'Met' THEN 1 ELSE 0 END +
  CASE WHEN hc.HBA1C_CHECK_12M = 'Met' THEN 1 ELSE 0 END +
  CASE WHEN hc.CHOLESTEROL_CHECK_12M = 'Met' THEN 1 ELSE 0 END +
  CASE WHEN hc.BP_CHECK_12M = 'Met' THEN 1 ELSE 0 END +
  CASE WHEN hc.SMOKING_CHECK_12M = 'Met' THEN 1 ELSE 0 END +
  CASE WHEN hc.ALCOHOL_CHECK_12M = 'Met' THEN 1 ELSE 0 END 
) AS SIX_COMP_COUNT

FROM {{ ref('int_smi_population_base') }} p
LEFT JOIN EXCEPTIONS e using (person_id)
LEFT JOIN HC_CHECKS hc using (person_id)
LEFT JOIN BMI b using (person_id)
LEFT JOIN GLUCOSE h using (person_id)
LEFT JOIN CHOL c using (person_id)
LEFT JOIN BP bp using (person_id)
LEFT JOIN SMOK s using (person_id)
LEFT JOIN ALC a using (person_id)