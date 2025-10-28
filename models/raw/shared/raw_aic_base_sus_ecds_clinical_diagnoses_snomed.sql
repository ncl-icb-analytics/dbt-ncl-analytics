-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_DIAGNOSES_SNOMED
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "IS_APPLICABLE_TO_FEMALES" as is_applicable_to_females,
    "IS_APPLICABLE_TO_MALES" as is_applicable_to_males,
    "IS_QUALIFIER_APPROVED" as is_qualifier_approved,
    "IS_NOTIFIABLE_DISEASE" as is_notifiable_disease,
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "CODE" as code,
    "IS_CODE_APPROVED" as is_code_approved,
    "IS_INJURY_RELATED" as is_injury_related,
    "EQUIVALENT_AE_CODE" as equivalent_ae_code,
    "SEQUENCE_NUMBER" as sequence_number,
    "IS_AEC_RELATED" as is_aec_related,
    "IS_ALLERGY_RELATED" as is_allergy_related,
    "IS_PRIMARY" as is_primary,
    "QUALIFIER" as qualifier
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_DIAGNOSES_SNOMED') }}
