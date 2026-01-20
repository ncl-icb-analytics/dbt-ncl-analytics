{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.episodes.commissioning.grouping.unbundled_hrg \ndbt: source(''sus_apc'', ''spell.episodes.commissioning.grouping.unbundled_hrg'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EPISODES_ID -> episodes_id\n  UNBUNDLED_HRG_ID -> unbundled_hrg_id\n  tariff -> tariff\n  dmicImportLogId -> dmic_import_log_id\n  code -> code\n  adult_cc_tariff_days -> adult_cc_tariff_days\n  multiple_applies -> multiple_applies"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "tariff" as tariff,
    "dmicImportLogId" as dmic_import_log_id,
    "code" as code,
    "adult_cc_tariff_days" as adult_cc_tariff_days,
    "multiple_applies" as multiple_applies
from {{ source('sus_apc', 'spell.episodes.commissioning.grouping.unbundled_hrg') }}
