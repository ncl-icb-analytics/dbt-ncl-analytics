-- Staging model for sus_apc.spell.episodes.patient.overseas_visitor
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "OVERSEAS_VISITOR_ID" as overseas_visitor_id,
    "classification" as classification,
    "start_date" as start_date,
    "end_date" as end_date,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.patient.overseas_visitor') }}
