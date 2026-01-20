{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.system.transaction.cds_recipients \ndbt: source(''sus_apc'', ''system.transaction.cds_recipients'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  dmicImportLogId -> dmic_import_log_id\n  CDS_RECIPIENTS_ID -> cds_recipients_id\n  cds_recipients -> cds_recipients"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "CDS_RECIPIENTS_ID" as cds_recipients_id,
    "cds_recipients" as cds_recipients
from {{ source('sus_apc', 'system.transaction.cds_recipients') }}
