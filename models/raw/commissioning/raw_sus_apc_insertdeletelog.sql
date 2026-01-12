{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.InsertDeleteLog \ndbt: source(''sus_apc'', ''InsertDeleteLog'') \nColumns:\n  PRIMARYKEY_ID -> primarykey_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicIsDeleted -> dmic_is_deleted\n  dmicDeletedImportLogId -> dmic_deleted_import_log_id"
    )
}}
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicIsDeleted" as dmic_is_deleted,
    "dmicDeletedImportLogId" as dmic_deleted_import_log_id
from {{ source('sus_apc', 'InsertDeleteLog') }}
