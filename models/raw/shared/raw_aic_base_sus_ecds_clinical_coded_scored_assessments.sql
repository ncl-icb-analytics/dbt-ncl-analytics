-- Raw layer model for aic.BASE_SUS__ECDS_CLINICAL_CODED_SCORED_ASSESSMENTS
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "DMIC_IMPORT_LOG_ID" as dmic_import_log_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ROWNUMBER_ID" as rownumber_id,
    "CODED_SCORED_ASSESSMENTS_ID" as coded_scored_assessments_id,
    "PERSON_SCORE" as person_score,
    "VALIDATION_TIMESTAMP" as validation_timestamp,
    "TOOL_TYPE_IS_CODE_APPROVED" as tool_type_is_code_approved,
    "TOOL_TYPE_CODE" as tool_type_code
from {{ source('aic', 'BASE_SUS__ECDS_CLINICAL_CODED_SCORED_ASSESSMENTS') }}
