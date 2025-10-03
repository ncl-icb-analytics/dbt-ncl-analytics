-- Raw layer model for sus_apc.system.transaction.cds_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmic_import_log_id,
    "CDS_RECIPIENTS_ID" as cds_recipients_id,
    "cds_recipients" as cds_recipients
from {{ source('sus_apc', 'system.transaction.cds_recipients') }}
