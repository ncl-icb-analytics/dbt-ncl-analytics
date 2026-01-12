{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.findings \ndbt: source(''sus_op'', ''appointment.clinical_coding.findings'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  FINDINGS_ID -> findings_id\n  code -> code\n  timestamp -> timestamp\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "FINDINGS_ID" as findings_id,
    "code" as code,
    "timestamp" as timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.findings') }}
