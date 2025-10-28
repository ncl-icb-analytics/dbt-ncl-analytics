-- Raw layer model for aic.INT_CONDITION
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MASTER_CONDITION_ID" as master_condition_id,
    "PERSON_ID" as person_id,
    "VISIT_OCCURRENCE_ID" as visit_occurrence_id,
    "VISIT_OCCURRENCE_TYPE" as visit_occurrence_type,
    "AGE_AT_EVENT" as age_at_event,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "CLINICAL_END_DATE" as clinical_end_date,
    "VISIT_PROBLEM_ORDER" as visit_problem_order,
    "CONDITION_CONCEPT_CODE" as condition_concept_code,
    "CONDITION_CONCEPT_NAME" as condition_concept_name,
    "DEFINITION_ID" as definition_id,
    "CONDITION_DEFINITION_NAME" as condition_definition_name,
    "DEFINITION_SOURCE" as definition_source
from {{ source('aic', 'INT_CONDITION') }}
