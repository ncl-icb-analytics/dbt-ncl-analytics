-- Staging model for sus_apc.spell.episodes.home_leave
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "HOME_LEAVE_ID" as home_leave_id,
    "start_date" as start_date,
    "start_time" as start_time,
    "end_date" as end_date,
    "end_time" as end_time,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.home_leave') }}
