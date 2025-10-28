-- Raw layer model for aic.BASE_SNOMED__CONCEPT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_SNOMED_CONCEPT_ID" as sk_snomed_concept_id,
    "SK_SNOMED_DESCRIPTION_ID" as sk_snomed_description_id,
    "SK_SNOMED_MODULE_ID" as sk_snomed_module_id,
    "PREFERRED_TERM" as preferred_term,
    "SK_SNOMED_DEFINITION_STATUS_ID" as sk_snomed_definition_status_id,
    "DEFINITION_STATUS" as definition_status,
    "IS_ACTIVE" as is_active,
    "IN_NATIONAL_DATASET" as in_national_dataset,
    "LAST_UPDATED" as last_updated
from {{ source('aic', 'BASE_SNOMED__CONCEPT') }}
