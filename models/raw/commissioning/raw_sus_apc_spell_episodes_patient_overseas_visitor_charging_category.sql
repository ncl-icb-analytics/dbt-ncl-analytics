{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.patient.overseas_visitor_charging_category \ndbt: source(''sus_apc'', ''spell.episodes.patient.overseas_visitor_charging_category'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  OVERSEAS_VISITOR_CHARGING_CATEGORY_ID -> overseas_visitor_charging_category_id\n  category -> category\n  applicable_from_date -> applicable_from_date\n  applicable_end_date -> applicable_end_date\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "OVERSEAS_VISITOR_CHARGING_CATEGORY_ID" as overseas_visitor_charging_category_id,
    "category" as category,
    "applicable_from_date" as applicable_from_date,
    "applicable_end_date" as applicable_end_date,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.episodes.patient.overseas_visitor_charging_category') }}
