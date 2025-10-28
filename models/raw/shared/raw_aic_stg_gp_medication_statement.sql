-- Raw layer model for aic.STG_GP__MEDICATION_STATEMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEDICATION_STATEMENT_ID" as medication_statement_id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "ENCOUNTER_ID" as encounter_id,
    "OBSERVATION_ID" as observation_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "AGE_AT_EVENT" as age_at_event,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "IS_ACTIVE" as is_active,
    "CANCELLATION_DATE" as cancellation_date,
    "MEDICATION_CONCEPT_ID" as medication_concept_id,
    "MEDICATION_CONCEPT_CODE" as medication_concept_code,
    "MEDICATION_CONCEPT_NAME" as medication_concept_name,
    "MEDICATION_CONCEPT_VOCABULARY" as medication_concept_vocabulary,
    "BNF_REFERENCE" as bnf_reference,
    "DOSE_INSTRUCTIONS" as dose_instructions,
    "QUANTITY_VALUE" as quantity_value,
    "QUANTITY_UNIT" as quantity_unit,
    "AUTHORISATION_TYPE_CONCEPT_ID" as authorisation_type_concept_id,
    "AUTHORISATION_TYPE_CONCEPT_CODE" as authorisation_type_concept_code,
    "AUTHORISATION_TYPE_CONCEPT_NAME" as authorisation_type_concept_name,
    "AUTHORISATION_TYPE_CONCEPT_VOCABULARY" as authorisation_type_concept_vocabulary
from {{ source('aic', 'STG_GP__MEDICATION_STATEMENT') }}
