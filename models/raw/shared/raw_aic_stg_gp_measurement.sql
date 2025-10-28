-- Raw layer model for aic.STG_GP__MEASUREMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEASUREMENT_ID" as measurement_id,
    "PERSON_ID" as person_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "DEFINITION_ID" as definition_id,
    "MEASUREMENT_NAME" as measurement_name,
    "OBSERVATION_CONCEPT_ID" as observation_concept_id,
    "OBSERVATION_CONCEPT_CODE" as observation_concept_code,
    "OBSERVATION_CONCEPT_NAME" as observation_concept_name,
    "OBSERVATION_CONCEPT_VOCABULARY" as observation_concept_vocabulary,
    "RESULT_VALUE" as result_value,
    "RESULT_VALUE_UNIT" as result_value_unit
from {{ source('aic', 'STG_GP__MEASUREMENT') }}
