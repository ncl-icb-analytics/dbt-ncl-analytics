{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.system.transaction.cds_copy_recipients \ndbt: source(''sus_apc'', ''spell.episodes.system.transaction.cds_copy_recipients'') \nColumns:\n  EPISODES_ID -> episodes_id\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  dmicImportLogId -> dmic_import_log_id\n  CDS_COPY_RECIPIENTS_ID -> cds_copy_recipients_id\n  cds_copy_recipients -> cds_copy_recipients"
    )
}}
select
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "CDS_COPY_RECIPIENTS_ID" as cds_copy_recipients_id,
    "cds_copy_recipients" as cds_copy_recipients
from {{ source('sus_apc', 'spell.episodes.system.transaction.cds_copy_recipients') }}
