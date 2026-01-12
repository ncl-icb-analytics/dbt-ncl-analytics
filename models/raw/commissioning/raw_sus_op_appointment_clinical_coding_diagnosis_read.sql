{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.diagnosis.read \ndbt: source(''sus_op'', ''appointment.clinical_coding.diagnosis.read'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  READ_ID -> read_id\n  code -> code\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "READ_ID" as read_id,
    "code" as code,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.diagnosis.read') }}
