{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.commissioning.grouping.unbundled_hrg \ndbt: source(''sus_apc'', ''spell.commissioning.grouping.unbundled_hrg'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  tariff -> tariff\n  dmicImportLogId -> dmic_import_log_id\n  UNBUNDLED_HRG_ID -> unbundled_hrg_id\n  code -> code\n  adult_cc_tariff_days -> adult_cc_tariff_days\n  multiple_applies -> multiple_applies\n  tariff_applied -> tariff_applied"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "tariff" as tariff,
    "dmicImportLogId" as dmic_import_log_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "code" as code,
    "adult_cc_tariff_days" as adult_cc_tariff_days,
    "multiple_applies" as multiple_applies,
    "tariff_applied" as tariff_applied
from {{ source('sus_apc', 'spell.commissioning.grouping.unbundled_hrg') }}
