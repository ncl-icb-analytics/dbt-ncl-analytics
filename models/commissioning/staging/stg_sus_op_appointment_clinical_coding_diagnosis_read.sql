-- Staging model for sus_op.appointment.clinical_coding.diagnosis.read
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "READ_ID" as read_id,
    "code" as code,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.diagnosis.read') }}
