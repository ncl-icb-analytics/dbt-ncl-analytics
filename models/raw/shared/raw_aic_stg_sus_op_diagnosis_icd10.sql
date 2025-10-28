-- Raw layer model for aic.STG_SUS__OP_DIAGNOSIS_ICD10
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "OP_DIAGNOSIS_ID" as op_diagnosis_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "ATTENDANCE_ID" as attendance_id,
    "ICD_ORDER" as icd_order,
    "ORGANISATION_ID" as organisation_id,
    "ORGANISATION_NAME" as organisation_name,
    "SUB_ORGANISATION_ID" as sub_organisation_id,
    "SUB_ORGANISATION_NAME" as sub_organisation_name,
    "ACTIVITY_DATE" as activity_date,
    "SOURCE_CONCEPT_CODE" as source_concept_code,
    "CONCEPT_CODE" as concept_code,
    "CONCEPT_NAME" as concept_name,
    "CONCEPT_VOCABULARY" as concept_vocabulary
from {{ source('aic', 'STG_SUS__OP_DIAGNOSIS_ICD10') }}
