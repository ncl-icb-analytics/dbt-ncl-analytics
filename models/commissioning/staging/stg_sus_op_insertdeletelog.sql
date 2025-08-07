-- Staging model for sus_op.InsertDeleteLog
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicIsDeleted" as dmicisdeleted,
    "dmicDeletedImportLogId" as dmicdeletedimportlogid
from {{ source('sus_op', 'InsertDeleteLog') }}
