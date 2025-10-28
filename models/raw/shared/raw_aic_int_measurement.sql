-- Raw layer model for aic.INT_MEASUREMENT
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "MEASUREMENT_ID" as measurement_id,
    "OBSERVATION_ID" as observation_id,
    "PERSON_ID" as person_id,
    "CLINICAL_EFFECTIVE_DATE" as clinical_effective_date,
    "DEFINITION_ID" as definition_id,
    "MEASUREMENT_NAME" as measurement_name,
    "MEASUREMENT_VALUE_AS_NUMBER" as measurement_value_as_number,
    "MEASUREMENT_UNIT" as measurement_unit,
    "MEASUREMENT_SOURCE_CONCEPT_CODE" as measurement_source_concept_code,
    "MEASUREMENT_SOURCE_CONCEPT_NAME" as measurement_source_concept_name,
    "MEASUREMENT_SOURCE_VALUE" as measurement_source_value,
    "MEASUREMENT_SOURCE_UNIT" as measurement_source_unit,
    "LOWER_LIMIT" as lower_limit,
    "UPPER_LIMIT" as upper_limit,
    "IS_NO_UNIT_MAPPING" as is_no_unit_mapping,
    "IS_BELOW_BOUND" as is_below_bound,
    "IS_ABOVE_BOUND" as is_above_bound
from {{ source('aic', 'INT_MEASUREMENT') }}
