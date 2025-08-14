-- Staging model for sus_ae.InsertDeleteLog
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicIsDeleted" as dmicisdeleted,
    "dmicDeletedImportLogId" as dmicdeletedimportlogid
from {{ source('sus_ae', 'InsertDeleteLog') }}
