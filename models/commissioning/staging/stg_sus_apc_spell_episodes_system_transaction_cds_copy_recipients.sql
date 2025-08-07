-- Staging model for sus_apc.spell.episodes.system.transaction.cds_copy_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "EPISODES_ID" as episodes_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "CDS_COPY_RECIPIENTS_ID" as cds_copy_recipients_id,
    "cds_copy_recipients" as cds_copy_recipients
from {{ source('sus_apc', 'spell.episodes.system.transaction.cds_copy_recipients') }}
