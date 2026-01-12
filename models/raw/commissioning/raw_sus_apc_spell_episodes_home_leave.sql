{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.home_leave \ndbt: source(''sus_apc'', ''spell.episodes.home_leave'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  HOME_LEAVE_ID -> home_leave_id\n  start_date -> start_date\n  start_time -> start_time\n  end_date -> end_date\n  end_time -> end_time\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
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
