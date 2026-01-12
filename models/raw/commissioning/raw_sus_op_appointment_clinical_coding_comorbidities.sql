{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.comorbidities \ndbt: source(''sus_op'', ''appointment.clinical_coding.comorbidities'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  COMORBIDITIES_ID -> comorbidities_id\n  code -> code\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "COMORBIDITIES_ID" as comorbidities_id,
    "code" as code,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.comorbidities') }}
