-- Staging model for sus_op.appointment.clinical_coding.findings
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "FINDINGS_ID" as findings_id,
    "code" as code,
    "timestamp" as timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_op', 'appointment.clinical_coding.findings') }}
