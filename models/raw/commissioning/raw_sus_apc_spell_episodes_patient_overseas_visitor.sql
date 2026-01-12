{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.patient.overseas_visitor \ndbt: source(''sus_apc'', ''spell.episodes.patient.overseas_visitor'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  OVERSEAS_VISITOR_ID -> overseas_visitor_id\n  classification -> classification\n  start_date -> start_date\n  end_date -> end_date\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "OVERSEAS_VISITOR_ID" as overseas_visitor_id,
    "classification" as classification,
    "start_date" as start_date,
    "end_date" as end_date,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.patient.overseas_visitor') }}
