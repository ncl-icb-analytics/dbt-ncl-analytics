-- Raw layer model for sus_apc.spell.episodes.commissioning.grouping.unbundled_hrg
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures
-- This is a 1:1 passthrough from source with standardized column names
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
