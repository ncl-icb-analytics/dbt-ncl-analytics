-- Staging model for sus_op.appointment.clinical_coding.coded_scored_assessments
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CODED_SCORED_ASSESSMENTS_ID" as coded_scored_assessments_id,
    "tool_type" as tool_type,
    "person_score" as person_score,
    "validation_timestamp" as validation_timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.coded_scored_assessments') }}
