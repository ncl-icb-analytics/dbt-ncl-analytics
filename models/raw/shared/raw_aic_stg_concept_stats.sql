-- Raw layer model for aic.STG_CONCEPT__STATS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "CONCEPT_NAME" as concept_name,
    "CONCEPT_CODE" as concept_code,
    "CONCEPT_VOCABULARY" as concept_vocabulary,
    "CONCEPT_CODE_COUNT" as concept_code_count,
    "UNIQUE_PATIENT_COUNT" as unique_patient_count,
    "LQ_VALUE" as lq_value,
    "MEDIAN_VALUE" as median_value,
    "UQ_VALUE" as uq_value,
    "PERCENT_HAS_RESULT_VALUE" as percent_has_result_value
from {{ source('aic', 'STG_CONCEPT__STATS') }}
