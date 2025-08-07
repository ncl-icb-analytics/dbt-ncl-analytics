-- Staging model for sus_apc.spell.episodes.clinical_coding.diagnosis.read
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "READ_ID" as read_id,
    "code" as code,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.clinical_coding.diagnosis.read') }}
