-- Raw layer model for aic.STG_GP__MEDICATION_ORDER
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEDICATION_ORDER_ID" as medication_order_id,
    "MEDICATION_STATEMENT_ID" as medication_statement_id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "ENCOUNTER_ID" as encounter_id,
    "OBSERVATION_ID" as observation_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "AGE_AT_EVENT" as age_at_event,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "MEDICATION_CONCEPT_ID" as medication_concept_id,
    "MEDICATION_CONCEPT_CODE" as medication_concept_code,
    "MEDICATION_CONCEPT_NAME" as medication_concept_name,
    "MEDICATION_CONCEPT_VOCABULARY" as medication_concept_vocabulary,
    "BNF_REFERENCE" as bnf_reference,
    "DOSE_INSTRUCTIONS" as dose_instructions,
    "QUANTITY_VALUE" as quantity_value,
    "QUANTITY_UNIT" as quantity_unit,
    "DURATION_DAYS" as duration_days,
    "ESTIMATED_COST" as estimated_cost,
    "ISSUE_METHOD_DESCRIPTION" as issue_method_description
from {{ source('aic', 'STG_GP__MEDICATION_ORDER') }}
