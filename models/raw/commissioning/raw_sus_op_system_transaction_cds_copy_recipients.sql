{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.system.transaction.cds_copy_recipients \ndbt: source(''sus_op'', ''system.transaction.cds_copy_recipients'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CDS_COPY_RECIPIENTS_ID -> cds_copy_recipients_id\n  cds_copy_recipients -> cds_copy_recipients\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CDS_COPY_RECIPIENTS_ID" as cds_copy_recipients_id,
    "cds_copy_recipients" as cds_copy_recipients,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'system.transaction.cds_copy_recipients') }}
