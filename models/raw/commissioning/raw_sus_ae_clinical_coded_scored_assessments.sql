-- Raw layer model for sus_ae.clinical.coded_scored_assessments
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "dmicImportLogId" as dmic_import_log_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ROWNUMBER_ID" as rownumber_id,
    "CODED_SCORED_ASSESSMENTS_ID" as coded_scored_assessments_id,
    "person_score" as person_score,
    "validation_timestamp" as validation_timestamp,
    "tool_type.is_code_approved" as tool_type_is_code_approved,
    "tool_type.code" as tool_type_code
from {{ source('sus_ae', 'clinical.coded_scored_assessments') }}
