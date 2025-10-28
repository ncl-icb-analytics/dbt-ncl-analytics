-- Raw layer model for aic.STG_GP__OBSERVATION
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "OBSERVATION_ID" as observation_id,
    "PARENT_OBSERVATION_ID" as parent_observation_id,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "ENCOUNTER_ID" as encounter_id,
    "ORGANISATION_CODE" as organisation_code,
    "ORGANISATION_NAME" as organisation_name,
    "AGE_AT_EVENT" as age_at_event,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "PROBLEM_END_DATE" as problem_end_date,
    "OBSERVATION_CONCEPT_ID" as observation_concept_id,
    "OBSERVATION_CONCEPT_CODE" as observation_concept_code,
    "OBSERVATION_CONCEPT_NAME" as observation_concept_name,
    "OBSERVATION_CONCEPT_VOCABULARY" as observation_concept_vocabulary,
    "RESULT_VALUE" as result_value,
    "RESULT_VALUE_UNIT" as result_value_unit,
    "IS_PROBLEM" as is_problem,
    "IS_REVIEW" as is_review,
    "IS_CONFIDENTIAL" as is_confidential
from {{ source('aic', 'STG_GP__OBSERVATION') }}
