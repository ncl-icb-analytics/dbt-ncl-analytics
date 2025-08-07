-- Staging model for sus_apc.system.transaction.cds_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "CDS_RECIPIENTS_ID" as cds_recipients_id,
    "cds_recipients" as cds_recipients
from {{ source('sus_apc', 'system.transaction.cds_recipients') }}
