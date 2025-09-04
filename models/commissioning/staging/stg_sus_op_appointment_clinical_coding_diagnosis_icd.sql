-- Staging model for sus_op.appointment.clinical_coding.diagnosis.icd
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "code" as code,
    "present_on_admission" as present_on_admission,
    "dmicImportLogId" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ICD_ID" as icd_id
from {{ source('sus_op', 'appointment.clinical_coding.diagnosis.icd') }}
