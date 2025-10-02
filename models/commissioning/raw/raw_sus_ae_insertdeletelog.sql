-- Raw layer model for sus_ae.InsertDeleteLog
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicIsDeleted" as dmic_is_deleted,
    "dmicDeletedImportLogId" as dmic_deleted_import_log_id
from {{ source('sus_ae', 'InsertDeleteLog') }}
