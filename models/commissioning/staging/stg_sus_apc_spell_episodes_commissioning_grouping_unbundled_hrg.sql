-- Staging model for sus_apc.spell.episodes.commissioning.grouping.unbundled_hrg
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EPISODES_ID" as episodes_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "tariff" as tariff,
    "dmicImportLogId" as dmicimportlogid,
    "code" as code,
    "adult_cc_tariff_days" as adult_cc_tariff_days,
    "multiple_applies" as multiple_applies
from {{ source('sus_apc', 'spell.episodes.commissioning.grouping.unbundled_hrg') }}
