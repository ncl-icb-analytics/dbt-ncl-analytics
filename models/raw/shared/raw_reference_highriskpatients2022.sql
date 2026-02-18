{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.HIGHRISKPATIENTS2022 \ndbt: source(''reference_analyst_managed'', ''HIGHRISKPATIENTS2022'') \nColumns:\n  PATIENT_ID -> patient_id\n  LOCAL_AUTHORITY -> local_authority\n  NEL_IP_ADMISSIONS_LAST_12_MONTHS -> nel_ip_admissions_last_12_months\n  HEART_FAILURE -> heart_failure\n  COPD -> copd\n  DEMENTIA -> dementia\n  END_STAGE_RENAL_FAILURE -> end_stage_renal_failure\n  SEVERE_INTERSTITIAL_LUNG_DISEASE -> severe_interstitial_lung_disease\n  PARKINSONS_DISEASE -> parkinsons_disease\n  CHRONIC_KIDNEY_DISEASE -> chronic_kidney_disease\n  LIVER_FAILURE -> liver_failure\n  ALCOHOL_DEPENDENCE -> alcohol_dependence\n  BRONCHIECTASIS -> bronchiectasis\n  ATRIAL_FIBRILLATION -> atrial_fibrillation\n  CEREBROVASCULAR_DISEASE -> cerebrovascular_disease\n  PERIPHERAL_VASCULAR_DISEASE -> peripheral_vascular_disease\n  PULMONARY_HEART_DISEASE -> pulmonary_heart_disease\n  CORONARY_HEART_DISEASE -> coronary_heart_disease\n  OSTEOPOROSIS -> osteoporosis\n  RHEUMATOID_ARTHRITIS -> rheumatoid_arthritis\n  CHRONIC_LIVER_DISEASE -> chronic_liver_disease"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "LOCAL_AUTHORITY" as local_authority,
    "NEL_IP_ADMISSIONS_LAST_12_MONTHS" as nel_ip_admissions_last_12_months,
    "HEART_FAILURE" as heart_failure,
    "COPD" as copd,
    "DEMENTIA" as dementia,
    "END_STAGE_RENAL_FAILURE" as end_stage_renal_failure,
    "SEVERE_INTERSTITIAL_LUNG_DISEASE" as severe_interstitial_lung_disease,
    "PARKINSONS_DISEASE" as parkinsons_disease,
    "CHRONIC_KIDNEY_DISEASE" as chronic_kidney_disease,
    "LIVER_FAILURE" as liver_failure,
    "ALCOHOL_DEPENDENCE" as alcohol_dependence,
    "BRONCHIECTASIS" as bronchiectasis,
    "ATRIAL_FIBRILLATION" as atrial_fibrillation,
    "CEREBROVASCULAR_DISEASE" as cerebrovascular_disease,
    "PERIPHERAL_VASCULAR_DISEASE" as peripheral_vascular_disease,
    "PULMONARY_HEART_DISEASE" as pulmonary_heart_disease,
    "CORONARY_HEART_DISEASE" as coronary_heart_disease,
    "OSTEOPOROSIS" as osteoporosis,
    "RHEUMATOID_ARTHRITIS" as rheumatoid_arthritis,
    "CHRONIC_LIVER_DISEASE" as chronic_liver_disease
from {{ source('reference_analyst_managed', 'HIGHRISKPATIENTS2022') }}
