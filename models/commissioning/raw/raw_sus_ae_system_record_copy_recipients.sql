-- Raw layer model for sus_ae.system.record.copy_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "copy_recipients" as copy_recipients,
    "COPY_RECIPIENTS_ID" as copy_recipients_id
from {{ source('sus_ae', 'system.record.copy_recipients') }}
