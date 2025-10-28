-- Raw layer model for aic.INT_GP_MEDICATION_STATEMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEDICATION_STATEMENT_ID" as medication_statement_id,
    "PERSON_ID" as person_id,
    "VISIT_OCCURRENCE_ID" as visit_occurrence_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "IS_ACTIVE" as is_active,
    "CANCELLATION_DATE" as cancellation_date,
    "MEDICATION_CONCEPT_CODE" as medication_concept_code,
    "MEDICATION_CONCEPT_NAME" as medication_concept_name,
    "VMP_CONCEPT_CODE" as vmp_concept_code,
    "VMP_CONCEPT_NAME" as vmp_concept_name,
    "VTM_CONCEPT_CODE" as vtm_concept_code,
    "VTM_CONCEPT_NAME" as vtm_concept_name,
    "BNF_REFERENCE" as bnf_reference,
    "DOSE_INSTRUCTIONS" as dose_instructions,
    "QUANTITY_VALUE" as quantity_value,
    "QUANTITY_UNIT" as quantity_unit,
    "AUTHORISATION_TYPE_CONCEPT_CODE" as authorisation_type_concept_code,
    "AUTHORISATION_TYPE_CONCEPT_NAME" as authorisation_type_concept_name,
    "AGE_AT_EVENT" as age_at_event
from {{ source('aic', 'INT_GP_MEDICATION_STATEMENT') }}
