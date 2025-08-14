-- Staging model for sus_ae.system.record.copy_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "copy_recipients" as copy_recipients,
    "COPY_RECIPIENTS_ID" as copy_recipients_id
from {{ source('sus_ae', 'system.record.copy_recipients') }}
