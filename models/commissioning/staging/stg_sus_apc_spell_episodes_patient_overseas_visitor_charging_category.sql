-- Staging model for sus_apc.spell.episodes.patient.overseas_visitor_charging_category
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "OVERSEAS_VISITOR_CHARGING_CATEGORY_ID" as overseas_visitor_charging_category_id,
    "category" as category,
    "applicable_from_date" as applicable_from_date,
    "applicable_end_date" as applicable_end_date,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_apc', 'spell.episodes.patient.overseas_visitor_charging_category') }}
